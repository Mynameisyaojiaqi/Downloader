# -*- coding: utf-8 -*-
"""
抓取网站内容
"""

"""
#提取到的请求信息：
    Request URL:https://rda.ucar.edu/cgi-bin/login
    Request Method:POST
    
Headers：
    Host: rda.ucar.edu
    origin:https://rda.ucar.edu
    referer:https://rda.ucar.edu/datasets/ds083.2/
    Upgrade-Insecure-Requests: 1
    User-Agent:Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36

Form Data:
    email: test
    passwd: testp
    remember: on
    do: login
    url: /datasets/ds083.2/?hash=!access
    
    


并上传至FTP中
"""
import urllib
import http.cookiejar as cookielib
import os
import sys
import requests
import datetime
import time
import subprocess
from ftplib import FTP
import socket

User_Agent = "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36"
header = {
        "Host":"rda.ucar.edu",
        "Origin":"https://rda.ucar.edu",
        "Referer":"https://rda.ucar.edu/datasets/ds083.2/index.html",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent":User_Agent,
        }
configure_file_dir = "E:/Data/"#配置文件所在目录
user_name = "lgy@aoe.ac.cn"#用户名
password = "lgy@aoe.ac.cn"#密码
login_url = "https://rda.ucar.edu/cgi-bin/login"#登录服务器地址
cookie_save_path = "E:/Data/python/NCAR/cookies/NCAR_Cookies.txt"#cookie存储地址
download_url_prefix = "https://rda.ucar.edu/data/ds083.2/grib2/"#文件所在url
download_file_dir = "E:/Data/python/NCAR/download_files/"#下载到该文件夹下
download_records_dir = "E:/Data/python/NCAR/records/"#下载记录
download_records_new = "E:/Data/python/NCAR/records/waiting_for_download.txt"
error_records = "E:/Data/python/NCAR/records/error.txt"
decompressed_file_dir = "E:/Data/python/NCAR/decompressed_files/"#解压路径
extractTools_Path = "E:/Data/python/NCAR/ExtractTools/"#解压工具路径
bat_path = "E:/Data/python/NCAR/temp_bat/"
FTP_user_name = "liguoyuan"#用户名
FTP_password = "liguoyuan"#密码
FTP_url = "172.16.2.224"#FTP地址




time_increment = datetime.timedelta(hours=6)#时间增量:6小时
#检查创建所需的目录
def Check_directory():
    global configure_file_dir
    configure_file_dir = os.path.dirname(sys.executable)
    configure_file_dir = configure_file_dir.replace('\\','/')
    if os.path.exists(configure_file_dir+"/NCAR_Config.cfg"):
        global user_name
        global password
        global cookie_save_path
        global download_file_dir
        global download_records_dir
        global download_records_new
        global error_records
        global decompressed_file_dir
        global extractTools_Path
        global bat_path
        ncar_config_file = open(configure_file_dir+"/NCAR_Config.cfg",'r')
        user_name = ncar_config_file.readline().split(' ')[1].replace('\n', '')
        password = ncar_config_file.readline().split(' ')[1].replace('\n', '')
        cookie_save_path = ncar_config_file.readline().split(' ')[1].replace('\n', '')
        download_file_dir = ncar_config_file.readline().split(' ')[1].replace('\n', '')
        download_records_dir = ncar_config_file.readline().split(' ')[1].replace('\n', '')
        download_records_new = ncar_config_file.readline().split(' ')[1].replace('\n', '')
        error_records = ncar_config_file.readline().split(' ')[1].replace('\n', '')
        decompressed_file_dir = ncar_config_file.readline().split(' ')[1].replace('\n', '')
        extractTools_Path = ncar_config_file.readline().split(' ')[1].replace('\n', '')
        bat_path = ncar_config_file.readline().split(' ')[1].replace('\n', '')
        ncar_config_file.close()
    else:
        if not os.path.exists(os.path.dirname(configure_file_dir)):
            os.makedirs(os.path.dirname(configure_file_dir))
        ncar_config_file = open(configure_file_dir+"/NCAR_Config.cfg",'w')
        ncar_config_file.write("用户名 "+user_name +'\n')
        ncar_config_file.write("密码 "+password +'\n')
        ncar_config_file.write("cookie本地存储路径 "+cookie_save_path +'\n')
        ncar_config_file.write("气象文件本地存储路径 "+download_file_dir +'\n')
        ncar_config_file.write("下载记录本地存储路径 "+download_records_dir +'\n')
        ncar_config_file.write("下载记录文件路径 "+download_records_new +'\n')
        ncar_config_file.write("下载错误记录 "+error_records +'\n')
        ncar_config_file.write("文件解压路径 "+decompressed_file_dir +'\n')
        ncar_config_file.write("ExtractTools所在路径 "+extractTools_Path +'\n')
        ncar_config_file.write("临时存放bat文件的路径 "+bat_path +'\n')
        ncar_config_file.write("FTP用户名" +FTP_user_name +'\n')
        ncar_config_file.write("FTP地址" +FTP_password +'\n')
        ncar_config_file.write("FTP地址 "+FTP_url)
        ncar_config_file.close()
    if not os.path.exists(download_file_dir):
        os.makedirs(download_file_dir)
    print("文件下载路径为:"+download_file_dir)
    if not os.path.exists(decompressed_file_dir):
        os.makedirs(decompressed_file_dir)
    print("文件解压路径为:"+decompressed_file_dir)
    if not os.path.exists(download_records_dir):
        os.makedirs(download_records_dir)
    print("下载记录文件路径为:"+download_records_dir)
    if not os.path.exists(os.path.dirname(cookie_save_path)):
        os.makedirs(os.path.dirname(cookie_save_path))
    print("cookies文件保存路径为:"+cookie_save_path)
    
    if not os.path.exists(download_records_new):
        print("下载记录文件不存在，需创建新的下载记录。请设定起始下载时间，程序将以此日期00时为起始时间，下载该日期之后的全部数据")
        isRight = False
        while not isRight:
            date_time_input = input("请依次输入年，月，日三个参数,以单个空格隔开\n")
            date_time_split = date_time_input.split()
            if len(date_time_split) != 3:
                continue
            try:  
                struct_time = datetime.datetime(int(date_time_split[0]),int(date_time_split[1]),int(date_time_split[2]))
            except ValueError:
                print("请输入正确的时间")
                continue
            else:
                isRight = True
        
        download_records_file = open(download_records_new,'w')
        download_records_file.write(date_time_input+" 00")
        download_records_file.close()
        
    if not os.path.exists(bat_path):
        os.makedirs(bat_path)
    print("bat临时存放路径为:"+bat_path)
    
    while not os.path.exists(extractTools_Path) or not os.path.exists(extractTools_Path+"wgrib2.exe"):
        print("请将ExtractTools放置于["+os.path.dirname(os.path.dirname(extractTools_Path))+"/]文件夹下后，按任意键继续")
        input("\n")
    
    print("目录检查完毕！")
#获取待下载文件对应的时间参数
def Read_data_time():
    isRight = False
    struct_time = ''
    try:
        download_records_file = open(download_records_new,'r')
        time_str = download_records_file.read()
        download_records_file.close()
        time_str_split = time_str.split()
        if len(time_str_split) == 4:
            time_str_split[3] = str(int(int(time_str_split[3])/6)*6)
            struct_time = datetime.datetime(int(time_str_split[0]),int(time_str_split[1]),int(time_str_split[2]),int(time_str_split[3]))
            isRight = True
    except:
        isRight = False
    if not isRight:
        print("读取待下载文件所对应的时间参数时发生错误，该文件所在的位置为"+download_records_new+" 该文件存储最新\
              的待下载文件所对应的时间，请检查该文件内容是否正确，或删除该文件重启本程序重新设置起始下载时间")
        exit()
    return struct_time
#将新的时间time写入记录文件
def Update_data_time(time):
    download_records_file = open(download_records_new,'w')
    download_records_file.write(str(time.year)+" "+str(time.month)+" "+str(time.day)+" "+str(time.hour))
    download_records_file.close()
#使用指定的用户名和密码登录服务器获取cookies，返回状态bool
def NCAR_Login(username,password):
    print("尝试登陆NCAR")
    post_data = {
            "email":username,
            "passwd":password,
            "remember":"on",
            "do":"login",
            }
    
    try:
        NCAR_cookies = cookielib.LWPCookieJar(filename = cookie_save_path)#
        NCAR_opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(NCAR_cookies))
        urllib.request.install_opener(NCAR_opener)
        post_data_ascii = urllib.parse.urlencode(post_data).encode('ascii')
        NCAR_response = NCAR_opener.open(login_url,post_data_ascii)
    except urllib.request.HTTPError:
        print("Login failed. Please check user name and password!(登录失败，请检查用户名和密码是否正确!)")
        return False
    except urllib.request.URLError:
        print("Login failed.(登录失败，网络不稳定，请稍后重试!)")
        return False
    else:
        NCAR_cookies.save()
        print("Login successful.(登录成功，cookies已保存至"+cookie_save_path+"!)")
        return True
    
#测试是否登陆成功(是否获取了正确的cookies)，返回状态bool
def isLogin():
    data_url = "https://rda.ucar.edu/datasets"
    if (not os.path.exists(cookie_save_path)):
        return False
    
    NCAR_cookies = cookielib.LWPCookieJar(filename = cookie_save_path)#
    NCAR_cookies.load()
    NCAR_opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(NCAR_cookies))
    
    for key in header: #增加多个header，把cookie放到header中，访问server时使用该cookie
        NCAR_opener.addheaders.append((key,header[key]))

    NCAR_response =  bytes.decode(NCAR_opener.open(data_url).read())
    if "dashboard" in NCAR_response and "sign out" in NCAR_response:
        return True
    return False

#从服务器指定地址【download_url】下载到指定本地位置【download_path】，返回状态bool
def Download_File(download_url,download_path,cookie):
    NCAR_response = requests.get(download_url,headers=header,cookies = NCAR_cookies,timeout=(3.05, 60)).content
    if len(NCAR_response) < 1000000:
        response_str =  bytes.decode(NCAR_response)
        if "dashboard" in response_str and "sign out" in response_str:
            return False
    else:
        # 写入文件,采用二进制写入文件
        with open(download_path,'wb') as f:
            f.write(NCAR_response)
        return True

def DecompressGrib2(ExtractTools_path, Grib2_file_name, decompress_path, bat_file_name):
    ExtractToolPath = ExtractTools_path.replace('/','\\')
    Grib2FileName = Grib2_file_name.replace('/','\\')
    outPath = decompress_path.replace('/','\\')
    batFileName = bat_file_name.replace('/','\\')
    ceil = [10, 20, 30, 50, 70, 100, 150, 200, 250, 300, 350, 400, 450, 500, 550, 600, 650, 700, 750, 800, 850, 900, 925, 950, 975, 1000]

    infile_Name = os.path.basename(Grib2FileName).split(".")[0];
    temp = ExtractToolPath + "wgrib2";
    #string temp = "wgrib";
    ss = '';

    batFile = open(batFileName,'w')
    for i in range(26):
        ss = temp + " -s " + Grib2FileName + " | find \":HGT:" + str(ceil[i]) + " mb:\" | " + temp + " -i " + Grib2FileName + " -bin " + outPath + infile_Name + "_" + str(ceil[i]) + "mba.HGT\n"
        batFile.write(ss)

        ss = temp + " -s " + Grib2FileName + " | find \":TMP:" + str(ceil[i]) + " mb:\" | " + temp + " -i " + Grib2FileName + " -bin " + outPath + infile_Name + "_" + str(ceil[i]) + "mba.TMP\n"
        batFile.write(ss)

        ss = temp + " -s " + Grib2FileName + " | find \":RH:" + str(ceil[i]) + " mb:\" | " + temp + " -i " + Grib2FileName + " -bin " + outPath + infile_Name + "_" + str(ceil[i]) + "mba.RH\n"
        batFile.write(ss)

    ss = temp + " -s " + Grib2FileName + " | find \":PWAT:\" | " + temp + " -i " + Grib2FileName + " -bin " + outPath + infile_Name + ".PWAT\n" #"_PWAT.dat"
    batFile.write(ss)

    ss = temp + " -s " + Grib2FileName + " | find \":PRES:surface:\" | " + temp + " -i " + Grib2FileName + " -bin " + outPath + infile_Name + ".PRES" #"_PWAT.dat";
    batFile.write(ss)
    
    batFile.close()
    print("开始解压...")
    #os.system(batFileName)
    #subprocess.Popen(bat_file_name,shell=False)
    subprocess.call(batFileName, creationflags=0x08000000)
    os.remove(batFileName)
    print(Grib2FileName+':解压完成(Decompression completed)!')
    
def write_string_to_txt(file_name,content):
    with open(file_name, "a") as f:
        f.write(content)
        

        

#从网上找了一个类用于处理FTP数据的上传、下载的类
class MyFTP:
    """
        ftp自动下载、自动上传脚本，可以递归目录操作
    """

    def __init__(self, host, port=21):
        """ 初始化 FTP 客户端
        参数:
                 host:ip地址

                 port:端口号
        """
        # print("__init__()---> host = %s ,port = %s" % (host, port))

        self.host = host
        self.port = port
        self.ftp = FTP()
        # 重新设置下编码方式
        self.ftp.encoding = 'gbk'
        self.log_file = open("log.txt", "a")
        self.file_list = []

    def login(self, username, password):
        """ 初始化 FTP 客户端
            参数:
                  username: 用户名

                 password: 密码
            """
        try:
            timeout = 60
            socket.setdefaulttimeout(timeout)
            # 0主动模式 1 #被动模式
            self.ftp.set_pasv(True)
            # 打开调试级别2，显示详细信息
            # self.ftp.set_debuglevel(2)

            self.debug_print('开始尝试连接到 %s' % self.host)
            self.ftp.connect(self.host, self.port)
            self.debug_print('成功连接到 %s' % self.host)

            self.debug_print('开始尝试登录到 %s' % self.host)
            self.ftp.login(username, password)
            self.debug_print('成功登录到 %s' % self.host)

            self.debug_print(self.ftp.welcome)
        except Exception as err:
            self.deal_error("FTP 连接或登录失败 ，错误描述为：%s" % err)
            pass

    def is_same_size(self, local_file, remote_file):
        """判断远程文件和本地文件大小是否一致

           参数:
             local_file: 本地文件

             remote_file: 远程文件
        """
        try:
            remote_file_size = self.ftp.size(remote_file)
        except Exception as err:
            # self.debug_print("is_same_size() 错误描述为：%s" % err)
            remote_file_size = -1

        try:
            local_file_size = os.path.getsize(local_file)
        except Exception as err:
            # self.debug_print("is_same_size() 错误描述为：%s" % err)
            local_file_size = -1

        self.debug_print('local_file_size:%d  , remote_file_size:%d' % (local_file_size, remote_file_size))
        if remote_file_size == local_file_size:
            return 1
        else:
            return 0

    def download_file(self, local_file, remote_file):
        """从ftp下载文件
            参数:
                local_file: 本地文件

                remote_file: 远程文件
        """
        self.debug_print("download_file()---> local_path = %s ,remote_path = %s" % (local_file, remote_file))

        if self.is_same_size(local_file, remote_file):
            self.debug_print('%s 文件大小相同，无需下载' % local_file)
            return
        else:
            try:
                self.debug_print('>>>>>>>>>>>>下载文件 %s ... ...' % local_file)
                buf_size = 1024
                file_handler = open(local_file, 'wb')
                self.ftp.retrbinary('RETR %s' % remote_file, file_handler.write, buf_size)
                file_handler.close()
            except Exception as err:
                self.debug_print('下载文件出错，出现异常：%s ' % err)
                return

    def download_file_tree(self, local_path, remote_path):
        """从远程目录下载多个文件到本地目录
                       参数:
                         local_path: 本地路径

                         remote_path: 远程路径
                """
        print("download_file_tree()--->  local_path = %s ,remote_path = %s" % (local_path, remote_path))
        try:
            self.ftp.cwd(remote_path)
        except Exception as err:
            self.debug_print('远程目录%s不存在，继续...' % remote_path + " ,具体错误描述为：%s" % err)
            return

        if not os.path.isdir(local_path):
            self.debug_print('本地目录%s不存在，先创建本地目录' % local_path)
            os.makedirs(local_path)

        self.debug_print('切换至目录: %s' % self.ftp.pwd())

        self.file_list = []
        # 方法回调
        self.ftp.dir(self.get_file_list)

        remote_names = self.file_list
        self.debug_print('远程目录 列表: %s' % remote_names)
        for item in remote_names:
            file_type = item[0]
            file_name = item[1]
            local = os.path.join(local_path, file_name)
            if file_type == 'd':
                print("download_file_tree()---> 下载目录： %s" % file_name)
                self.download_file_tree(local, file_name)
            elif file_type == '-':
                print("download_file()---> 下载文件： %s" % file_name)
                self.download_file(local, file_name)
            self.ftp.cwd("..")
            self.debug_print('返回上层目录 %s' % self.ftp.pwd())
        return True

    def upload_file(self, local_file, remote_file):
        """从本地上传文件到ftp

           参数:
             local_path: 本地文件

             remote_path: 远程文件
        """
        if not os.path.isfile(local_file):
            self.debug_print('%s 不存在' % local_file)
            return

        if self.is_same_size(local_file, remote_file):
            self.debug_print('跳过相等的文件: %s' % local_file)
            return

        buf_size = 1024
        file_handler = open(local_file, 'rb')
        self.ftp.storbinary('STOR %s' % remote_file, file_handler, buf_size)
        file_handler.close()
        self.debug_print('上传: %s' % local_file + "成功!")

    def upload_file_tree(self, local_path, remote_path):
        """从本地上传目录下多个文件到ftp
           参数:

             local_path: 本地路径

             remote_path: 远程路径
        """
        if not os.path.isdir(local_path):
            self.debug_print('本地目录 %s 不存在' % local_path)
            return

        self.ftp.cwd(remote_path)
        self.debug_print('切换至远程目录: %s' % self.ftp.pwd())

        local_name_list = os.listdir(local_path)
        for local_name in local_name_list:
            src = os.path.join(local_path, local_name)
            if os.path.isdir(src):
                try:
                    self.ftp.mkd(local_name)
                except Exception as err:
                    self.debug_print("目录已存在 %s ,具体错误描述为：%s" % (local_name, err))
                self.debug_print("upload_file_tree()---> 上传目录： %s" % local_name)
                self.upload_file_tree(src, local_name)
            else:
                self.debug_print("upload_file_tree()---> 上传文件： %s" % local_name)
                self.upload_file(src, local_name)
        self.ftp.cwd("..")

    def close(self):
        """ 退出ftp
        """
        self.debug_print("close()---> FTP退出")
        self.ftp.quit()
        self.log_file.close()

    def debug_print(self, s):
        """ 打印日志
        """
        self.write_log(s)

    def deal_error(self, e):
        """ 处理错误异常
            参数：
                e：异常
        """
        log_str = '发生错误: %s' % e
        self.write_log(log_str)
        sys.exit()

    def write_log(self, log_str):
        """ 记录日志
            参数：
                log_str：日志
        """
        time_now = time.localtime()
        date_now = time.strftime('%Y-%m-%d', time_now)
        format_log_str = "%s ---> %s \n " % (date_now, log_str)
        print(format_log_str)
        self.log_file.write(format_log_str)

    def get_file_list(self, line):
        """ 获取文件列表
            参数：
                line：
        """
        file_arr = self.get_file_name(line)
        # 去除  . 和  ..
        if file_arr[1] not in ['.', '..']:
            self.file_list.append(file_arr)

    def get_file_name(self, line):
        """ 获取文件名
            参数：
                line：
        """
        pos = line.rfind(':')
        while (line[pos] != ' '):
            pos += 1
        while (line[pos] == ' '):
            pos += 1
        file_arr = [line[0], line[pos:]]
        return file_arr



if __name__ == "__main__":
    #创建所需目录
    Check_directory()
    my_ftp = MyFTP(FTP_url)
    my_ftp.login(FTP_user_name, FTP_password)
    while True:
        try:
            #获取待下载文件的时间
            struct_time = Read_data_time()
            
            #登录网站获取并保存cookies
            if not isLogin():
                if not NCAR_Login(user_name, password):
                    exit()
    
            #load cookie
            print("加载cookies...")
            NCAR_cookies = cookielib.LWPCookieJar(filename = cookie_save_path)
            NCAR_cookies.load()
            
            year_str = "%04d"%(struct_time.year)
            month_str = "%02d"%(struct_time.month)
            day_str = "%02d"%(struct_time.day)
            hour_str = "%02d"%(struct_time.hour)
            
            download_file_name = "fnl_"+year_str+month_str+day_str+"_"+hour_str+"_00.grib2"
            download_url = download_url_prefix + year_str+"/"+year_str+"."+month_str+"/"+download_file_name
            download_path = download_file_dir + year_str + "/" + month_str + "/" + download_file_name
            if not os.path.exists(os.path.dirname(download_path)):
                os.makedirs(os.path.dirname(download_path))
            
            print("待下载数据文件名为："+download_file_name)
            print("待下载文件url:"+download_url)
            print("尝试下载")
            if Download_File(download_url,download_path,NCAR_cookies):
                print("下载成功，文件已保存至："+download_path)
                try:
                    outPath = decompressed_file_dir + year_str + "/" + month_str + "/" + download_file_name.split('.')[0] + "/"
                    if not os.path.exists(outPath):
                        os.makedirs(outPath)
                    DecompressGrib2(extractTools_Path, download_path, outPath, bat_path+download_file_name.split('.')[0]+'.bat')
                except:
                    write_string_to_txt(error_records,download_path+":Decompress failed.\n")
                    print(error_records,download_path+":Decompress failed.错误记录保存至："+error_records)
                print("将数据上传至ftp:" )        
                FTPuploadfilepath = decompressed_file_dir + year_str + "/" + month_str + "/"
                my_ftp.upload_file_tree(FTPuploadfilepath, "/Atm_Data/")
                Update_data_time(struct_time + time_increment)
            else:
                print("数据不存在，1小时后继续尝试下载...")
                time.sleep(3600)
        except urllib.request.URLError:
            print("当前网络不稳定，5分钟后继续尝试下载...")
            time.sleep(300)
        except requests.ReadTimeout:
            print("下载超时，10秒后继续尝试下载...")
            time.sleep(10)
        except requests.ConnectionError:
            print("网络连接失败,1分钟后继续尝试下载...")
            time.sleep(60)
        except:
            print("未知原因导致下载文件/上传ftp失败，2秒钟后下载下一个文件...")
            #更新时间戳，下载下一个文件
            Update_data_time(struct_time + time_increment)
            time.sleep(2)
   
            
    
    
    
    
    
    
    
    
    