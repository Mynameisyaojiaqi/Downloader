from __future__ import print_function
from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QMainWindow, QApplication,QFormLayout,QLineEdit, QPushButton
import requests as r
import shapefile
import base64
import itertools
import json
import netrc
import ssl
import sys
import os
import time

try:
    from urllib.parse import urlparse
    from urllib.request import urlopen, Request, build_opener, HTTPCookieProcessor
    from urllib.error import HTTPError, URLError
except ImportError:
    from urlparse import urlparse
    from urllib2 import urlopen, Request, HTTPError, URLError, build_opener, HTTPCookieProcessor

bounding_box = ''
polygon = ''
filename_filter = ''
url_list = []

CMR_URL = 'https://cmr.earthdata.nasa.gov'
URS_URL = 'https://urs.earthdata.nasa.gov'
CMR_PAGE_SIZE = 2000
CMR_FILE_URL = ('{0}/search/granules.json?provider=NSIDC_ECS'
                '&sort_key[]=start_date&sort_key[]=producer_granule_id'
                '&scroll=true&page_size={1}'.format(CMR_URL, CMR_PAGE_SIZE))
class Logger(object):
    def __init__(self, filename="Default.log"):
        self.terminal = sys.stdout
        self.log = open(filename, "w")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        pass



def Create_file_list(filepath):
    filelist = []
    for home, dirs, files in os.walk(filepath):
        for filename in files:
            if filename.endswith(('h5')):
                singlefile = os.path.join(home, filename)
                # print(os.path.getsize(singlefile))
                if os.path.getsize(singlefile) > 10000:
                    filelist.append(filename)
    return filelist


def get_credentials(url):
    """Get user credentials from .netrc or prompt for input."""
    credentials = None
    errprefix = ''
    try:
        info = netrc.netrc()
        username, account, password = info.authenticators(urlparse(URS_URL).hostname)
        errprefix = 'netrc error: '
    except Exception as e:
        if (not ('No such file' in str(e))):
            print('netrc error: {0}'.format(str(e)))
        username = None
        password = None

    while not credentials:
        if not username:
            username = 'Yao_Rs_2018'
            password = 'Yao_Rs_2018'
        credentials = '{0}:{1}'.format(username, password)
        credentials = base64.b64encode(credentials.encode('ascii')).decode('ascii')

        if url:
            try:
                req = Request(url)
                req.add_header('Authorization', 'Basic {0}'.format(credentials))
                opener = build_opener(HTTPCookieProcessor())
                opener.open(req)
            except HTTPError:
                print(errprefix + 'Incorrect username or password')
                errprefix = ''
                credentials = None
                username = None
                password = None

    return credentials


def build_version_query_params(version):
    desired_pad_length = 3
    if len(version) > desired_pad_length:
        print('Version string too long: "{0}"'.format(version))
        quit()

    version = str(int(version))  # Strip off any leading zeros
    query_params = ''

    while len(version) <= desired_pad_length:
        padded_version = version.zfill(desired_pad_length)
        query_params += '&version={0}'.format(padded_version)
        desired_pad_length -= 1
    return query_params


def build_cmr_query_url(short_name, version, time_start, time_end,
                        bounding_box=None, polygon=None,
                        filename_filter=None):
    params = '&short_name={0}'.format(short_name)
    params += build_version_query_params(version)
    params += '&temporal[]={0},{1}'.format(time_start, time_end)
    if polygon:
        params += '&polygon={0}'.format(polygon)
    elif bounding_box:
        params += '&bounding_box={0}'.format(bounding_box)
    if filename_filter:
        option = '&options[producer_granule_id][pattern]=true'
        params += '&producer_granule_id[]={0}{1}'.format(filename_filter, option)
    return CMR_FILE_URL + params


def cmr_download(urls, outpath='./Download'):
    """Download files from list of urls."""

    filelist = Create_file_list(outpath)
    print('完成储存路径内文件检索')

    if not urls:
        return

    url_count = len(urls)
    print('Downloading {0} files...'.format(url_count))
    credentials = None

    for index, url in enumerate(urls, start=1):
        if not credentials and urlparse(url).scheme == 'https':
            credentials = get_credentials(url)

        filename = url.split('/')[-1]

        if filename not in filelist and filename.endswith(("h5")):

            print('{0}/{1}: {2}'.format(str(index).zfill(len(str(url_count))),
                                        url_count,
                                        filename))
            count = 5
            while count:
                try:
                    # In Python 3 we could eliminate the opener and just do 2 lines:
                    # resp = requests.get(url, auth=(username, password))
                    # open(filename, 'wb').write(resp.content)
                    req = Request(url)
                    if credentials:
                        req.add_header('Authorization', 'Basic {0}'.format(credentials))
                    opener = build_opener(HTTPCookieProcessor())
                    data = opener.open(req).read()
                    filename1 = outpath + '/' + filename
                    open(filename1, 'wb').write(data)
                    break
                except HTTPError as e:
                    print('HTTP error {0}, {1}'.format(e.code, e.reason))
                    print("当前网络不稳定，5分钟后继续尝试下载...")
                    time.sleep(300)
                    count -= 1
                except URLError as e:
                    print('URL error: {0}'.format(e.reason))
                    print("当前网络不稳定，5分钟后继续尝试下载...")
                    time.sleep(300)
                    count -= 1
                except IOError:
                    raise
                except KeyboardInterrupt:
                    quit()
                except:
                    print("未知原因导致下载文件失败,1分钟后下载下一个文件...")
                    time.sleep(60)
                    break


def cmr_filter_urls(search_results):
    """Select only the desired data files from CMR response."""
    if 'feed' not in search_results or 'entry' not in search_results['feed']:
        return []

    entries = [e['links']
               for e in search_results['feed']['entry']
               if 'links' in e]
    # Flatten "entries" to a simple list of links
    links = list(itertools.chain(*entries))

    urls = []
    unique_filenames = set()
    for link in links:
        if 'href' not in link:
            # Exclude links with nothing to download
            continue
        if 'inherited' in link and link['inherited'] is True:
            # Why are we excluding these links?
            continue
        if 'rel' in link and 'data#' not in link['rel']:
            # Exclude links which are not classified by CMR as "data" or "metadata"
            continue

        if 'title' in link and 'opendap' in link['title'].lower():
            # Exclude OPeNDAP links--they are responsible for many duplicates
            # This is a hack; when the metadata is updated to properly identify
            # non-datapool links, we should be able to do this in a non-hack way
            continue

        filename = link['href'].split('/')[-1]
        if filename in unique_filenames:
            # Exclude links with duplicate filenames (they would overwrite)
            continue
        unique_filenames.add(filename)

        urls.append(link['href'])

    return urls


def cmr_search(short_name, version, time_start, time_end,
               bounding_box='', polygon='', filename_filter=''):
    """Perform a scrolling CMR query for files matching input criteria."""
    cmr_query_url = build_cmr_query_url(short_name=short_name, version=version,
                                        time_start=time_start, time_end=time_end,
                                        bounding_box=bounding_box,
                                        polygon=polygon, filename_filter=filename_filter)
    print('Querying for data:\n\t{0}\n'.format(cmr_query_url))

    cmr_scroll_id = None
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    try:
        urls = []
        while True:
            req = Request(cmr_query_url)
            if cmr_scroll_id:
                req.add_header('cmr-scroll-id', cmr_scroll_id)
            response = urlopen(req, context=ctx)
            if not cmr_scroll_id:
                # Python 2 and 3 have different case for the http headers
                headers = {k.lower(): v for k, v in dict(response.info()).items()}
                cmr_scroll_id = headers['cmr-scroll-id']
                hits = int(headers['cmr-hits'])
                if hits > 0:
                    print('Found {0} matches.'.format(hits))
                else:
                    print('Found no matches.')
            search_page = response.read()
            search_page = json.loads(search_page.decode('utf-8'))
            url_scroll_results = cmr_filter_urls(search_page)
            if not url_scroll_results:
                break
            if hits > CMR_PAGE_SIZE:
                print('.', end='')
                sys.stdout.flush()
            urls += url_scroll_results

        if hits > CMR_PAGE_SIZE:
            print()
        return urls
    except KeyboardInterrupt:
        quit()


def main(short_name,version,time_start,time_end,polygon,outpath):
    sys.stdout = Logger('./printlogtest.txt')
    url_list = []

    if 'shp' in polygon:
        r = shapefile.Reader(polygon)
        bounding_box = ','.join([str(x) for x in r.bbox])
    else:
        bounding_box = polygon

    if not os.path.exists(outpath):
        os.mkdir(outpath)

    if not url_list:
        url_list = cmr_search(short_name, version, time_start, time_end,
                              bounding_box=bounding_box,filename_filter=filename_filter)
    cmr_download(url_list, outpath=outpath)


def gedi_finder(product, bbox):
    # Define the base CMR granule search url, including LPDAAC provider name and max page size (2000 is the max allowed)
    cmr = "https://cmr.earthdata.nasa.gov/search/granules.json?pretty=true&provider=LPDAAC_ECS&page_size=2000&concept_id="

    # Set up dictionary where key is GEDI shortname + version and value is CMR Concept ID
    concept_ids = {'GEDI01_B.002': 'C1908344278-LPDAAC_ECS',
                   'GEDI02_A.002': 'C1908348134-LPDAAC_ECS',
                   'GEDI02_B.002': 'C1908350066-LPDAAC_ECS'}

    # CMR uses pagination for queries with more features returned than the page size
    page = 1
    bbox = bbox.replace(' ', '')  # Remove any white spaces
    try:
        # Send GET request to CMR granule search endpoint w/ product concept ID, bbox & page number, format return as json
        cmr_response = r.get(f"{cmr}{concept_ids[product]}&bounding_box={bbox}&pageNum={page}").json()['feed']['entry']

        # If 2000 features are returned, move to the next page and submit another request, and append to the response
        while len(cmr_response) % 2000 == 0:
            page += 1
            cmr_response += r.get(f"{cmr}{concept_ids[product]}&bounding_box={bbox}&pageNum={page}").json()['feed'][
                'entry']

        # CMR returns more info than just the Data Pool links, below use list comprehension to return a list of DP links
        return [c['links'][0]['href'] for c in cmr_response]
    except:
        # If the request did not complete successfully, print out the response from CMR
        print(r.get(f"{cmr}{concept_ids[product]}&bounding_box={bbox.replace(' ', '')}&pageNum={page}").json())

def download_GEDI(short_name,version,polygon,outpath1):
    # args = args_parse()
    # short_name = args['short_name']
    # version = args['version']
    # polygon = args['polygon']
    # outpath1 = args['outpath']  # './Download'

    # short_name = 'GEDI02_B'
    # version = '002'
    # polygon = '-73.65,-12.64,-47.81,9.7'
    # outpath1 = 'E:/Download'

    if 'shp' in polygon:
        r1 = shapefile.Reader(polygon)
        bbx = ','.join([str(x) for x in r1.bbox])
    else:
        bbx = polygon

    if not os.path.exists(outpath1):
        os.mkdir(outpath1)

        # product = 'GEDI02_B.002'
    # bbox = '-73.65,-12.64,-47.81,9.7'
    product = short_name + '.' + version
    print(product, bbx)
    granules = gedi_finder(product, bbx)
    print('完成检索准备下载！')
    # print(f"{len(granules)} {product} Version 2 granules found.")
    cmr_download(granules, outpath=outpath1)


#class MainWIndow(QMainWindow):
class MainWIndow(QLineEdit):
    def __init__(self, parent=None):
        super(MainWIndow, self).__init__(parent)
        self.resize(800, 200)
        self.setWindowTitle('激光测高卫星数据下载模块')
        # 设置无边框窗口样式
        #self.setWindowFlags(Qt.FramelessWindowHint)
        self.setWindowFlags(Qt.Widget)
        #子窗口，窗口无按钮 ，但有标题，可注释掉观察效果
        #self.setWindowFlags(Qt.SubWindow)
        self.setObjectName("MainWindow")
        self.setStyleSheet("#MainWindow{border-image:url(./image/Earth_background1.jpg);}")
        #self.setWindowOpacity(0.9)#设置整体窗口透明度
        #self.setAttribute(Qt.WA_TranslucentBackground)  # 设置窗口背景透明
        self.btnPress1 = QPushButton("下载数据")
        flo = QFormLayout()
        self.pProductLineEdit = QLineEdit(self)
        self.pVersionLineEdit = QLineEdit(self)
        self.pBeginLineEdit = QLineEdit(self)
        self.pEndLineEdit = QLineEdit(self)
        self.pPolygonLineEdit = QLineEdit(self)
        self.pOutpathLineEdit = QLineEdit(self)
        flo.addRow("产品名称", self.pProductLineEdit)
        flo.addRow("版本号", self.pVersionLineEdit)
        flo.addRow("开始时间", self.pBeginLineEdit)
        flo.addRow("结束时间", self.pEndLineEdit)
        flo.addRow("框选区域经纬度", self.pPolygonLineEdit)
        flo.addRow("输出路径", self.pOutpathLineEdit)
        flo.addWidget(self.btnPress1)
        self.pProductLineEdit.setPlaceholderText("例如：ATL03/ATL06/ATL08/GEDI_1B/GEDI_2A/GEDI_2B")
        self.pVersionLineEdit.setPlaceholderText("例如：001/002/003/004/005")
        self.pBeginLineEdit.setPlaceholderText("例如：2019-05-06T16:59:32Z")
        self.pEndLineEdit.setPlaceholderText("例如：2022-04-03T02:29:18Z")
        self.pPolygonLineEdit.setPlaceholderText("左上、右下，例如：-73.65,-12.64,-47.81,9.7或输入shp")
        self.pOutpathLineEdit.setPlaceholderText("下载文件保存路径")
        # 设置显示效果
        self.pProductLineEdit.setEchoMode(QLineEdit.Normal)
        self.pVersionLineEdit.setEchoMode(QLineEdit.Normal)
        self.pBeginLineEdit.setEchoMode(QLineEdit.Normal)
        self.pEndLineEdit.setEchoMode(QLineEdit.Normal)
        self.pPolygonLineEdit.setEchoMode(QLineEdit.Normal)
        self.pOutpathLineEdit.setEchoMode(QLineEdit.Normal)
        self.setLayout(flo)
        self.btnPress1.clicked.connect(self.btnPress1_Clicked)

    def btnPress1_Clicked(self):
        product = self.pProductLineEdit.text()
        version = self.pVersionLineEdit.text()
        start_time = self.pBeginLineEdit.text()
        end_time = self.pEndLineEdit.text()
        ploygon = self.pPolygonLineEdit.text()
        outpath = self.pOutpathLineEdit.text()

        if 'ATL' in product:
            main(product,version,start_time,end_time,ploygon,outpath)
        else:
            download_GEDI(product, version, ploygon, outpath)

if __name__ == '__main__':
    app = QApplication(sys.argv)
    win = MainWIndow()
    win.show()
    sys.exit(app.exec_())
