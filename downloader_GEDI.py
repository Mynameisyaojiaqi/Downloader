# -*- coding: utf-8 -*-
"""
Created on Wed Apr 27 09:47:09 2022

@author: Yao
"""
import base64
import argparse
import netrc
import shapefile
import requests as r
import time
import os
try:
    from urllib.parse import urlparse
    from urllib.request import urlopen, Request, build_opener, HTTPCookieProcessor
    from urllib.error import HTTPError, URLError
except ImportError:
    from urlparse import urlparse
    from urllib2 import urlopen, Request, HTTPError, URLError, build_opener, HTTPCookieProcessor

CMR_URL = 'https://cmr.earthdata.nasa.gov'
URS_URL = 'https://urs.earthdata.nasa.gov'
bounding_box = ''
polygon = ''


def args_parse():
    # construct the argument parse and parse the arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--short_name", type=str,  help='GEDI**')
    parser.add_argument("--version", type=str)
    parser.add_argument('--polygon', nargs='+', type=str)
    parser.add_argument('--outpath', type=str)
    #args, unparsed = parser.parse_known_args()
    args = vars(parser.parse_args())
    return args

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
            cmr_response += r.get(f"{cmr}{concept_ids[product]}&bounding_box={bbox}&pageNum={page}").json()['feed']['entry']
        
        # CMR returns more info than just the Data Pool links, below use list comprehension to return a list of DP links
        return [c['links'][0]['href'] for c in cmr_response]
    except:
        # If the request did not complete successfully, print out the response from CMR
        print(r.get(f"{cmr}{concept_ids[product]}&bounding_box={bbox.replace(' ', '')}&pageNum={page}").json())


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




        
if __name__ == '__main__':
    
    
    args = args_parse()
    short_name = args['short_name']
    version = args['version']
    polygon = args['polygon']
    outpath1 = args['outpath']#'./Download'
    
    # short_name = 'GEDI02_B'
    # version = '002'
    # polygon = '-73.65,-12.64,-47.81,9.7'
    # outpath1 = 'E:/Download'
    
    
    if 'shp' in polygon:
        r1 = shapefile.Reader(polygon)
        bbx = ','.join([str(x) for x in r1.bbox])
    else:
        bbx = ','.join([str(x) for x in polygon])
        
    if not os.path.exists(outpath1):
        os.mkdir(outpath1)    
    
    
    
    # product = 'GEDI02_B.002'           
    # bbox = '-73.65,-12.64,-47.81,9.7'  
    product = short_name + '.' + version
    print(product, bbx)
    granules = gedi_finder(product, bbx)
    print('完成检索准备下载！')
    #print(f"{len(granules)} {product} Version 2 granules found.")
    cmr_download(granules,outpath=outpath1)
    
    
    
    
    
    
    
    