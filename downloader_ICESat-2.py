# -*- coding: utf-8 -*-
"""
Created on Sun Apr  3 11:47:45 2022

@author: Yao
"""

# 20220403 Yao
# 更新：增加矢量筛选模块

from __future__ import print_function
import shapefile
import argparse
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

# short_name = 'ATL08'
# version = '004'
# time_start = '2019-05-06T16:59:32Z'
# time_end = '2022-4-3T02:29:18Z'
# input_shape = r"E:\2023QQCT\America_area.shp" 
# r = shapefile.Reader(input_shape)
#bounding_box = r.bbox
#bounding_box = ['-168.1277619999163', '58.181991470553214', '-140.96223399999894', '71.39065600027442']
# str1 = ','.join([str(x) for x in r.bbox])
# print(str1)
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




def args_parse():
    # construct the argument parse and parse the arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--short_name", type=str,  help='ATL**')
    parser.add_argument("--version", type=str)
    parser.add_argument('--time_start', type=str, help='The starting time')
    parser.add_argument('--time_end', type=str, help='The ending time')
    parser.add_argument('--polygon', type=str)
    parser.add_argument('--bounding_box', type=str)
    parser.add_argument('--outpath', type=str)
    #args, unparsed = parser.parse_known_args()
    args = vars(parser.parse_args())
    return args

def Create_file_list(filepath):
    filelist = []
    for home, dirs, files in os.walk(filepath):
        for filename in files:
            if filename.endswith(('h5')):
                singlefile = os.path.join(home,filename)
                #print(os.path.getsize(singlefile))
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
                    filename1 = outpath+ '/' + filename
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



def main():
    sys.stdout = Logger('./printlogtest.txt')
    
    global short_name, version, time_start, time_end, bounding_box, \
        polygon, filename_filter, url_list

    # Supply some default search parameters, just for testing purposes.
    # These are only used if the parameters aren't filled in up above.
    
    args = args_parse()
    short_name = args['short_name']
    version = args['version']
    time_start = args['time_start']
    time_end = args['time_end'] 
    polygon = args['polygon']
    bounding_box = args['bounding_box']
    outpath = args['outpath']#'./Download'
    
    if 'shp' in bounding_box:
        r = shapefile.Reader(bounding_box)
        bounding_box = ','.join([str(x) for x in r.bbox])
    else:
        bounding_box = bounding_box[1:-1]
        
    if not os.path.exists(outpath):
        os.mkdir(outpath)    
    
    
    if not url_list:
        url_list = cmr_search(short_name, version, time_start, time_end,
                              bounding_box=bounding_box,
                              polygon=polygon, filename_filter=filename_filter)

    cmr_download(url_list,outpath=outpath)


if __name__ == '__main__':
    main()














