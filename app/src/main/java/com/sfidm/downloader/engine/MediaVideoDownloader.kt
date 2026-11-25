package com.sfidm.downloader.engine

import android.content.Context
import com.sfidm.downloader.model.DownloadMode
import com.sfidm.downloader.model.FileCategory
import com.sfidm.downloader.model.MediaQuality
import com.yausername.ffmpeg.FFmpeg
import com.yausername.youtubedl_android.YoutubeDL
import com.yausername.youtubedl_android.YoutubeDLRequest
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import okhttp3.OkHttpClient
import okhttp3.Request
import java.io.File
import java.util.concurrent.TimeUnit

class MediaVideoDownloader(
    private val context: Context,
    private val directDownloader: MultiThreadDownloader
) {

    private val initMutex = Mutex()
    private var engineReady = false

    private val webClient = OkHttpClient.Builder()
        .connectTimeout(20, TimeUnit.SECONDS)
        .readTimeout(25, TimeUnit.SECONDS)
        .followRedirects(true)
        .followSslRedirects(true)
        .build()

    data class MediaOutcome(
        val filePath: String,
        val fileSize: Long,
        val category: FileCategory,
        val resolvedName: String
    )

    data class EngineUpdateResult(
        val status: YoutubeDL.UpdateStatus?,
        val channelUsed: String,
        val errorMessage: String? = null
    )

    data class PinterestMedia(
        val directUrl: String,
        val suggestedName: String,
        val isVideo: Boolean
    )

    suspend fun ensureInitialized() = initMutex.withLock {
        if (engineReady) return
        withContext(Dispatchers.IO) {
            YoutubeDL.getInstance().init(context.applicationContext)
            runCatching { FFmpeg.getInstance().init(context.applicationContext) }
            engineReady = true
        }
    }

    suspend fun updateEngine(): EngineUpdateResult = withContext(Dispatchers.IO) {
        ensureInitialized()
        // Try Nightly channel first (most up-to-date for YouTube/FB/TikTok/Pinterest), then Stable
        val nightlyAttempt = runCatching {
            YoutubeDL.getInstance().updateYoutubeDL(
                context.applicationContext,
                YoutubeDL.UpdateChannel.NIGHTLY
            )
        }
        if (nightlyAttempt.isSuccess) {
            return@withContext EngineUpdateResult(
                status = nightlyAttempt.getOrNull(),
                channelUsed = "Nightly"
            )
        }

        val stableAttempt = runCatching {
            YoutubeDL.getInstance().updateYoutubeDL(
                context.applicationContext,
                YoutubeDL.UpdateChannel.STABLE
            )
        }
        if (stableAttempt.isSuccess) {
            return@withContext EngineUpdateResult(
                status = stableAttempt.getOrNull(),
                channelUsed = "Stable"
            )
        }

        val err = nightlyAttempt.exceptionOrNull()?.message
            ?: stableAttempt.exceptionOrNull()?.message
            ?: "Network or GitHub API unreachable"
        EngineUpdateResult(
            status = null,
            channelUsed = "None",
            errorMessage = err
        )
    }

    fun cancelProcess(processId: String) {
        runCatching { YoutubeDL.getInstance().destroyProcessById(processId) }
    }

    fun resolveShortUrl(rawUrl: String): String {
        val trimmed = rawUrl.trim()
        val lower = trimmed.lowercase()
        val shortHosts = listOf(
            "pin.it", "vm.tiktok.com", "vt.tiktok.com", "fb.watch",
            "youtu.be", "t.co", "bit.ly", "tinyurl.com", "instagr.am", "redd.it"
        )
        if (shortHosts.none { lower.contains(it) }) {
            return normalizePinterestUrl(trimmed)
        }

        return try {
            val req = Request.Builder()
                .url(trimmed)
                .header(
                    "User-Agent",
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
                )
                .build()
            webClient.newCall(req).execute().use { resp ->
                val finalUrl = resp.request.url.toString()
                normalizePinterestUrl(finalUrl.ifBlank { trimmed })
            }
        } catch (_: Exception) {
            trimmed
        }
    }

    private fun normalizePinterestUrl(url: String): String {
        val lower = url.lowercase()
        if (!lower.contains("pinterest.")) return url
        val pinMatch = Regex("/pin/(\\d+)").find(url)
        return if (pinMatch != null) {
            val pinId = pinMatch.groupValues[1]
            "https://www.pinterest.com/pin/$pinId/"
        } else {
            url.substringBefore("?")
        }
    }

    suspend fun downloadMedia(
        processId: String,
        url: String,
        customName: String,
        quality: MediaQuality,
        isVault: Boolean,
        speedLimitProvider: () -> Long,
        onProgress: (
            percent: Float,
            downloadedBytes: Long,
            totalBytes: Long,
            speedBytesPerSec: Long,
            detail: String,
            resolvedTitle: String?
        ) -> Unit
    ): MediaOutcome = withContext(Dispatchers.IO) {
        onProgress(0f, 0L, 0L, 0L, "Resolving link...", null)
        val resolvedUrl = resolveShortUrl(url)
        val isPinterest = resolvedUrl.contains("pinterest.", ignoreCase = true) ||
            resolvedUrl.contains("pin.it", ignoreCase = true)

        if (isPinterest) {
            // Try yt-dlp first for video pins; if it's an Image Pin or yt-dlp fails, fall back to direct Pinterest parser
            val ytAttempt = runCatching {
                executeYtDlp(
                    processId = processId,
                    targetUrl = resolvedUrl,
                    customName = customName,
                    quality = quality,
                    isVault = isVault,
                    speedLimitBytesPerSec = speedLimitProvider(),
                    onProgress = { pct, detail, title ->
                        onProgress(pct, 0L, 0L, 0L, detail, title)
                    }
                )
            }
            if (ytAttempt.isSuccess) {
                return@withContext ytAttempt.getOrThrow()
            }

            onProgress(5f, 0L, 0L, 0L, "Extracting Pinterest media...", null)
            val pinMedia = extractPinterestDirectMedia(resolvedUrl, customName)
            if (pinMedia != null) {
                val directOutcome = directDownloader.download(
                    url = pinMedia.directUrl,
                    requestedFileName = pinMedia.suggestedName,
                    threads = if (pinMedia.isVideo) 8 else 4,
                    isVault = isVault,
                    speedLimitProvider = speedLimitProvider
                ) { downloaded, total, speed, resolvedName, _ ->
                    val pct = if (total > 0L) (downloaded.toFloat() / total.toFloat()) * 100f else 50f
                    val detail = if (speed > 0L) "${pct.toInt()}%" else ""
                    onProgress(pct, downloaded, total, speed, detail, resolvedName)
                }
                return@withContext MediaOutcome(
                    filePath = directOutcome.filePath,
                    fileSize = directOutcome.fileSize,
                    category = directOutcome.category,
                    resolvedName = directOutcome.resolvedName
                )
            }

            throw ytAttempt.exceptionOrNull()
                ?: IllegalStateException("Could not extract video or image from Pinterest link")
        }

        executeYtDlp(
            processId = processId,
            targetUrl = resolvedUrl,
            customName = customName,
            quality = quality,
            isVault = isVault,
            speedLimitBytesPerSec = speedLimitProvider(),
            onProgress = { pct, detail, title ->
                onProgress(pct, 0L, 0L, 0L, detail, title)
            }
        )
    }

    private suspend fun executeYtDlp(
        processId: String,
        targetUrl: String,
        customName: String,
        quality: MediaQuality,
        isVault: Boolean,
        speedLimitBytesPerSec: Long,
        onProgress: (percent: Float, detail: String, resolvedTitle: String?) -> Unit
    ): MediaOutcome = withContext(Dispatchers.IO) {
        ensureInitialized()

        val tempBase = File(context.cacheDir, "sfidm_media_$processId")
        if (!tempBase.exists()) tempBase.mkdirs()

        val baseName = if (customName.isNotBlank()) {
            FileExportHelper.sanitizeFileName(customName.substringBeforeLast("."))
        } else {
            "%(title).70s"
        }
        val outputTemplate = File(tempBase, "$baseName.%(ext)s").absolutePath

        val request = YoutubeDLRequest(targetUrl)
        request.addOption("-o", outputTemplate)
        request.addOption("--no-mtime")
        request.addOption("--no-playlist")
        request.addOption("--restrict-filenames")
        request.addOption("--concurrent-fragments", "4")
        request.addOption(
            "--user-agent",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
        )

        if (speedLimitBytesPerSec > 0L) {
            val kbps = (speedLimitBytesPerSec / 1024L).coerceAtLeast(64L)
            request.addOption("--limit-rate", "${kbps}K")
        }

        when (quality) {
            MediaQuality.BEST -> {
                request.addOption("-f", "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best")
                request.addOption("--merge-output-format", "mp4")
            }
            MediaQuality.HD_1080 -> {
                request.addOption("-f", "bestvideo[height<=1080][ext=mp4]+bestaudio[ext=m4a]/best[height<=1080][ext=mp4]/best[height<=1080]/best")
                request.addOption("--merge-output-format", "mp4")
            }
            MediaQuality.HD_720 -> {
                request.addOption("-f", "bestvideo[height<=720][ext=mp4]+bestaudio[ext=m4a]/best[height<=720][ext=mp4]/best[height<=720]/best")
                request.addOption("--merge-output-format", "mp4")
            }
            MediaQuality.SD_480 -> {
                request.addOption("-f", "bestvideo[height<=480][ext=mp4]+bestaudio[ext=m4a]/best[height<=480][ext=mp4]/best[height<=480]/best")
                request.addOption("--merge-output-format", "mp4")
            }
            MediaQuality.AUDIO_MP3 -> {
                request.addOption("-x")
                request.addOption("--audio-format", "mp3")
                request.addOption("--audio-quality", "0")
            }
        }

        var detectedTitle: String? = null

        try {
            YoutubeDL.getInstance().execute(request, processId) { progress, etaInSeconds, line ->
                if (detectedTitle == null && line.contains("Destination:")) {
                    val rawDest = line.substringAfter("Destination:").trim()
                    val name = File(rawDest).name
                    if (name.isNotBlank()) detectedTitle = name
                }
                val cleanPercent = if (progress >= 0f) progress.coerceIn(0f, 100f) else 0f
                val detail = buildString {
                    if (cleanPercent > 0f) append("${cleanPercent.toInt()}%")
                    if (etaInSeconds > 0L) {
                        if (isNotEmpty()) append(" • ")
                        append("ETA ${etaInSeconds}s")
                    }
                }
                onProgress(cleanPercent, detail, detectedTitle)
            }

            val downloadedFile = tempBase.listFiles()
                ?.filter { it.isFile && !it.name.endsWith(".part") && !it.name.endsWith(".ytdl") }
                ?.maxByOrNull { it.length() }
                ?: throw IllegalStateException("Downloaded media file not found")

            val category = FileCategory.detect(downloadedFile.name, DownloadMode.MEDIA_VIDEO, quality)
            val destDir = if (isVault) {
                FileExportHelper.getVaultDir(context)
            } else {
                FileExportHelper.getCategoryWorkingDir(context, category)
            }

            val finalDest = File(destDir, downloadedFile.name)
            if (finalDest.exists()) finalDest.delete()
            downloadedFile.copyTo(finalDest, overwrite = true)

            if (!isVault) {
                FileExportHelper.exportToPublicCategoryFolder(context, finalDest, category, finalDest.name)
            }

            MediaOutcome(
                filePath = finalDest.absolutePath,
                fileSize = finalDest.length(),
                category = category,
                resolvedName = finalDest.name
            )
        } finally {
            tempBase.deleteRecursively()
        }
    }

    private fun extractPinterestDirectMedia(pinUrl: String, customName: String): PinterestMedia? {
        return try {
            val req = Request.Builder()
                .url(pinUrl)
                .header(
                    "User-Agent",
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
                )
                .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
                .header("Accept-Language", "en-US,en;q=0.9")
                .build()

            val html = webClient.newCall(req).execute().use { resp ->
                if (!resp.isSuccessful) return null
                resp.body?.string().orEmpty()
            }
            if (html.isBlank()) return null

            val pinId = Regex("/pin/(\\d+)").find(pinUrl)?.groupValues?.getOrNull(1)
                ?: System.currentTimeMillis().toString()

            val rawTitle = extractHtmlTitle(html)
            val baseTitle = customName.ifBlank {
                rawTitle?.take(55)?.ifBlank { "Pinterest_$pinId" } ?: "Pinterest_$pinId"
            }

            // 1. Check for direct Pinterest MP4 video URL in HTML/JSON
            val videoMatch = Regex("https://v[0-9a-z]*\\.pinimg\\.com/videos/[^\"'\\\\\\s<>]+\\.mp4")
                .find(html)
            if (videoMatch != null) {
                val videoUrl = videoMatch.value.replace("\\u002F", "/")
                val safeName = FileExportHelper.sanitizeFileName("${baseTitle.substringBeforeLast(".")}.mp4")
                return PinterestMedia(directUrl = videoUrl, suggestedName = safeName, isVideo = true)
            }

            // 2. Check for highest-resolution original image URL
            val origMatch = Regex("https://i\\.pinimg\\.com/originals/[^\"'\\\\\\s<>]+\\.(?:jpg|jpeg|png|webp|gif)")
                .find(html)
            if (origMatch != null) {
                val imgUrl = origMatch.value.replace("\\u002F", "/")
                val ext = imgUrl.substringAfterLast(".").substringBefore("?").lowercase().ifBlank { "jpg" }
                val safeName = FileExportHelper.sanitizeFileName("${baseTitle.substringBeforeLast(".")}.$ext")
                return PinterestMedia(directUrl = imgUrl, suggestedName = safeName, isVideo = false)
            }

            // 3. Fallback to 736x / 564x image URL and try upgrading to /originals/
            val sizedMatch = Regex("https://i\\.pinimg\\.com/(?:736x|564x|474x)/[^\"'\\\\\\s<>]+\\.(?:jpg|jpeg|png|webp)")
                .find(html)
            if (sizedMatch != null) {
                val sizedUrl = sizedMatch.value.replace("\\u002F", "/")
                val upgradedUrl = sizedUrl.replace(Regex("/(?:736x|564x|474x)/"), "/originals/")
                val finalImgUrl = if (isUrlReachable(upgradedUrl)) upgradedUrl else sizedUrl
                val ext = finalImgUrl.substringAfterLast(".").substringBefore("?").lowercase().ifBlank { "jpg" }
                val safeName = FileExportHelper.sanitizeFileName("${baseTitle.substringBeforeLast(".")}.$ext")
                return PinterestMedia(directUrl = finalImgUrl, suggestedName = safeName, isVideo = false)
            }

            // 4. Final fallback: og:image meta tag
            val ogImage = Regex("<meta[^>]+property=[\"']og:image[\"'][^>]+content=[\"']([^\"']+)[\"']", RegexOption.IGNORE_CASE)
                .find(html)?.groupValues?.getOrNull(1)
            if (!ogImage.isNullOrBlank() && ogImage.startsWith("http")) {
                val ext = ogImage.substringAfterLast(".").substringBefore("?").lowercase()
                    .takeIf { it in listOf("jpg", "jpeg", "png", "webp") } ?: "jpg"
                val safeName = FileExportHelper.sanitizeFileName("${baseTitle.substringBeforeLast(".")}.$ext")
                return PinterestMedia(directUrl = ogImage, suggestedName = safeName, isVideo = false)
            }

            null
        } catch (_: Exception) {
            null
        }
    }

    private fun extractHtmlTitle(html: String): String? {
        val ogTitle = Regex("<meta[^>]+property=[\"']og:title[\"'][^>]+content=[\"']([^\"']+)[\"']", RegexOption.IGNORE_CASE)
            .find(html)?.groupValues?.getOrNull(1)
        val raw = ogTitle ?: Regex("<title[^>]*>([^<]+)</title>", RegexOption.IGNORE_CASE)
            .find(html)?.groupValues?.getOrNull(1)
        return raw?.replace(" | Pinterest", "", ignoreCase = true)
            ?.replace(" - Pinterest", "", ignoreCase = true)
            ?.replace("&amp;", "&")
            ?.replace("&#39;", "'")
            ?.replace("&quot;", "\"")
            ?.trim()
            ?.takeIf { it.isNotBlank() && !it.equals("Pinterest", ignoreCase = true) }
    }

    private fun isUrlReachable(url: String): Boolean {
        return try {
            val req = Request.Builder().url(url).head().build()
            webClient.newCall(req).execute().use { it.isSuccessful }
        } catch (_: Exception) {
            false
        }
    }

    companion object {
        private val mediaHosts = listOf(
            "youtube.com", "youtu.be", "m.youtube.com", "music.youtube.com",
            "facebook.com", "fb.watch", "fb.com", "m.facebook.com",
            "tiktok.com", "vm.tiktok.com", "vt.tiktok.com",
            "instagram.com", "instagr.am",
            "twitter.com", "x.com",
            "reddit.com", "redd.it",
            "vimeo.com", "dailymotion.com", "soundcloud.com",
            "pinterest.com", "pin.it", "twitch.tv", "bilibili.com"
        )

        private val directExtensions = listOf(
            ".zip", ".rar", ".7z", ".apk", ".pdf", ".iso", ".exe",
            ".dmg", ".tar", ".gz", ".bin", ".docx", ".xlsx", ".pptx",
            ".jpg", ".jpeg", ".png", ".webp", ".gif"
        )

        fun isLikelyMediaPage(url: String): Boolean {
            val lower = url.lowercase()
            if (lower.contains(".m3u8") || lower.contains(".mpd")) return true
            if (mediaHosts.any { lower.contains(it) }) return true
            val path = lower.substringBefore("?").substringBefore("#")
            if (directExtensions.any { path.endsWith(it) }) return false
            if (path.endsWith(".mp4") || path.endsWith(".mkv") || path.endsWith(".mp3")) return false
            return true
        }
    }
}
