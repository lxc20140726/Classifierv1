# Emby 缩略图规范

## 命名约定

- 主缩略图：`{name}-thumb.jpg`
- 横向图像：`landscape.jpg`
- 背景图像：`backdrop.jpg`

## 格式

- 首选格式：`JPEG`
- 仅对需要透明度的资源使用 `PNG`。

## 推荐尺寸

- 缩略图：`16:9`，通常为 `1280x720`
- 背景：`1920x1080`

## FFmpeg 示例

```bash
ffmpeg -i input.mp4 -ss 00:00:10 -frames:v 1 -vf "scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2" output-thumb.jpg
```

此示例在 10 秒处提取一帧，并将其标准化为适合 Emby 风格媒体封面的 16:9 缩略图尺寸。

## 文件夹结构指南

- 电影：每部电影一个文件夹。
- 电视剧：按剧集组织，然后按季划分子文件夹。
- 尽可能将封面文件与媒体文件放在一起，以便 Emby 可以自动检测它们。