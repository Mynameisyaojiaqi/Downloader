# Downloader
Download tools for obtaining data such as Google, remote sensing images and satellite laser altimetry through python

# Downloader_of_satellite_laser_altimetry
Satellite laser altimetry data download module, which supports the screening and downloading of ICESat-2(ATL03/06/08) and GEDI(GESI1B\2A\2B) data.
(卫星激光测高数据下载模块，支持ICESat-2（ATL03/06/08）以及GEDI(GESI1B\2A\2B)等数据的筛选与下载,支持经纬度、获取时间等筛选方式)

![ba48bbf4f6a89e0db6fb24b1a0c2ed7](https://user-images.githubusercontent.com/39584551/165676352-07862b07-9f32-4f50-92bd-dd8d81df3de3.png)

# downloader_ICESat-2
ICESat-2 data downloader
（ICESat-2数据下载器）

## Run
downloader_ICESat-2.exe --short_name ATL08 --version 004 --time_start 2019-05-06T16:59:32Z --time_end 2022-4-3T02:29:18Z --bounding_box E:/2023QQCT/America_area.shp --outpath E:/Download

or

python downloader_ICESat-2.exe --short_name ATL08 --version 004 --time_start 2019-05-06T16:59:32Z --time_end 2022-4-3T02:29:18Z --bounding_box -168.1277619999163 58.181991470553214 -140.96223399999894 71.39065600027442 --outpath E:/Download


# downloader_GEDI
GEDI data downloader
（GEDI 数据下载器）

## Run

downloader_GEDI.exe --short_name GEDI02_B --version 002 --polygon E:/**.shp --outpath E:/Download

or

downloader_GEDI.exe --short_name GEDI02_B --version 002 --polygon -73.65,-12.64,-47.81,9.7 --outpath E:/Download


