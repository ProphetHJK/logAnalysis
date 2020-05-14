# -*- coding:utf-8 -*-
import os
import re
import sqlite3
import time

# srcDir = './srcDir'
# dstDir = './dstDir'

def datetime_timestamp(dt):
    #dt为字符串
    #中间过程，一般都需要将字符串转化为时间数组
    time.strptime(dt, '%Y-%m-%d %H:%M:%S')
    ## time.struct_time(tm_year=2012, tm_mon=3, tm_mday=28, tm_hour=6, tm_min=53, tm_sec=40, tm_wday=2, tm_yday=88, tm_isdst=-1)
    #将"2012-03-28 06:53:40"转化为时间戳
    s = time.mktime(time.strptime(dt, '%Y-%m-%d %H:%M:%S'))
    return int(s)

start_time = datetime_timestamp('2020-05-13 05:00:00')  #运行程序的当地时间
end_time = datetime_timestamp('2020-05-14 05:00:00')  #运行程序的当地时间
start_EOB_time = datetime_timestamp('2020-05-01 05:00:00')
end_EOB_time = datetime_timestamp('2020-05-14 05:00:00')
time_type = 'data_time'
EOB_month = 5

path = r'G:\沙特现场问题'  # 日志父路径
dir_name = '台区5-集中器日志-SXE2030180003822-0513-升级前' #日志文件夹名
# out_dir = path+ '\\output\\'
out_dir = os.path.join(path, dir_name)+ '\\output\\'
if not os.path.exists(out_dir):
    os.makedirs(out_dir)

auto_search = 0  #自动查找path路径下日志文件夹
change_name = 1  #改名合并(已经能自动判断)
log_analy = 1    #日志分析(需先改名合并)
once_analy = 1   #一次抄表成功率分析(需先改名合并)
db_analy = 0     #数据库分析
old_version = 0  #1.7.2日志
path += '\\'+dir_name+'\\'
# path = os.getcwd()
log_name = 'all.log'  # 日志合并文件文件名
profile_name = dir_name+'_'+time_type+'_'+str(start_time)+'-'+str(end_time)+'.csv'  # 数据库导出文件名
srwf_name = dir_name+'_3762.log'   # 3762报文文件名

### 第一部分：日志处理
## 改名
filelist = os.listdir(path)

filesum = 0  # message总数
startnum = 0  # 重命名开始计数

if change_name == True:
    if os.path.isfile(os.path.join(path,log_name)):
        print('{0} already exist, escape change name'.format(log_name))
        print('--------------------------------')

    else:
        # 判断是否已经改名
        change_flag = False
        for i in filelist:
            if re.match(r'messages.{3}', i):
                change_flag = True
                break
        if not change_flag:
            # 将单个后缀改名
            for i in filelist:
                # 判断是否为日志文件
                isMessage = re.match(r'messages.{0,2}', i)
                if isMessage:
                    filesum += 1
                    # 判断文件名长度为10
                    if len(i) == 10:
                        messagename = 'messages.0%d' % startnum
                        os.rename(path+i, path+messagename)
                        # print(path+i,'-->',path+messagename)
                        if startnum > 9:
                            print("too many 10 length messages")
                            os.exit()
                        else:
                            startnum += 1


            # 更新列表
            filelist = os.listdir(path)
            print('file count is ', filesum)
            filesum -= 1

            # 倒序改名
            for i in filelist:
                # 判断是否为日志文件
                isMessage = re.match(r'messages.{0,2}', i)
                if isMessage:
                    messagename = 'messages.%03d' % filesum
                    # print(path+i,'-->',path+messagename)
                    os.rename(path+i, path+messagename)
                    filesum -= 1
        ## 合并文件
        # 新建合并文件，二进制写入，行尾符不会自动识别系统，或者newline='\n'
        all_log = open(path+log_name, 'wb')
        # 更新列表
        filelist = os.listdir(path)
        # 合并文件
        for i in filelist:
            # 判断是否为日志文件
            isMessage = re.match(r'messages.{0,2}', i)
            if isMessage:
                x = open(path+i, 'rb')
                all_log.write(x.read())
                x.close()
        all_log.close()

        print('change name success')
        print('--------------------------------')

## 日志分析功能
if log_analy == True:
    if not os.path.isfile(os.path.join(path,log_name)):
        print('{0} not exist, please change name first'.format(log_name))
        print('--------------------------------')
    else:
        # 正则表达式
        pattern_line = r'((.*SRWF -> 3_47,TX.*\n(.*DC: [0-9A-F]*)*\n)|(.*SRWF.*\n(.*DC: [0-9A-F]{0,}\n)*.*unpack3762.*\n))'
        # 转化为对象
        pattern = re.compile(pattern_line)
        all_log = open(path+log_name, 'r', encoding='UTF-8')
        result1 = pattern.findall(all_log.read())
        all_log.close()
        srwf_info = open(out_dir+srwf_name, 'w', newline='\n', encoding='UTF-8')
        # 匹配的所有内容输出到文件
        for i in result1:
            srwf_info.write(i[0])  # 第一个捕获组，最外面的括号
            srwf_info.write('\n')

        srwf_info.close()

        if old_version == True:
            # 正则替换，删除RX后的一条记录
            def change(value):
                return value.group(1)+'\n'


            srwf_info = open(out_dir+srwf_name, 'r', newline='\n', encoding='UTF-8')
            result2 = re.sub(r'(.*SRWF -> 3_47,RX).*\n(.*\n)', change, srwf_info.read())
            # result2 = srwf_info.read()
            srwf_info.close()

            srwf_info = open(out_dir+srwf_name, 'w', newline='\n', encoding='UTF-8')
            srwf_info.write(result2)
            srwf_info.close()

        print('3762log processing success')
        print('--------------------------------')

## 一次抄表成功率分析
if once_analy == True:
    if not os.path.isfile(os.path.join(path,log_name)):
        print('{0} not exist, please change name first'.format(log_name))
        print('--------------------------------')
    else:
        # 统计一次抄表成功率
        # 正则表达式
        pattern_line = r'AFN : 13, FN : 1'
        # 转化为对象
        pattern = re.compile(pattern_line)
        all_log = open(path+log_name, 'r', encoding='UTF-8')
        result1 = pattern.findall(all_log.read())
        current_count = len(result1)

        pattern_line = r'03032020010313010000000000|03032020010313010000000002'
        # 转化为对象
        pattern = re.compile(pattern_line)
        all_log = open(path+log_name, 'r', encoding='UTF-8')
        result1 = pattern.findall(all_log.read())
        wrong_count = len(result1)
        print("wrong_count:{0}".format(wrong_count))
        print("total_count:{0}".format(current_count))
        print("success_rate:{:.2f}%".format((current_count-wrong_count)/current_count*100))
        print('--------------------------------')

### 第二部分：数据库处理
if db_analy == True:
    conn = sqlite3.connect(path+'dc.db')
    print('open dc.db successfully')
    sql_c = conn.cursor()
    sql_command = '''
            SELECT
            id,
            serial_no,
            IFNULL( AverageDataProfile.num, 0 ) AS "AverageDataProfile.count",
            IFNULL( EnergyProfile.num, 0 ) AS "EnergyProfile.count",
            IFNULL( EOBProfile.num, 0 ) AS "EOBProfile.count",
            IFNULL( LoadProfile.num, 0 ) AS "LoadProfile.count" 
        FROM
            meter
            LEFT OUTER JOIN ( SELECT count( 1 ) AS num, meter_id FROM data_AverageDataProfile_sd WHERE {4} >= {0} AND {4} <= {1} GROUP BY meter_id ) AS AverageDataProfile ON AverageDataProfile.meter_id = meter.id
            LEFT OUTER JOIN ( SELECT count( 1 ) AS num, meter_id FROM data_EnergyProfile_sd WHERE {4} >= {0} AND {4} <= {1} GROUP BY meter_id ) AS EnergyProfile ON EnergyProfile.meter_id = meter.id
            LEFT OUTER JOIN ( SELECT count( 1 ) AS num, meter_id FROM data_EOB_DataProfile_sd WHERE {4} >= {2} AND {4} <= {3} GROUP BY meter_id ) AS EOBProfile ON EOBProfile.meter_id = meter.id
            LEFT OUTER JOIN ( SELECT count( 1 ) AS num, meter_id FROM data_LoadProfile_sd WHERE {4} >= {0} AND {4} <= {1} GROUP BY meter_id ) AS LoadProfile ON LoadProfile.meter_id = meter.id 
        WHERE
    status = 1;
    '''.format(start_time, end_time, start_EOB_time, end_EOB_time, time_type)  # 格式化sql语句
    print(sql_command)
    cursor = sql_c.execute(
        sql_command
    )
    sql_file = open(out_dir+profile_name, 'w')
    sql_file.write(
        'id,serial_no,AverageDataProfile,EnergyProfile,EOBProfile,LoadProfile\n')

    AverageDataProfile_sum = 0
    EnergyProfile_sum = 0
    EOBProfile_sum = 0
    LoadProfile_sum = 0
    total_id = 0  # 表的总数量
    success_count = 0  # 有数据的表的数量

    for row in cursor:
        total_id += 1
        AverageDataProfile_sum += row[2]
        EnergyProfile_sum += row[3]
        EOBProfile_sum += row[4]
        LoadProfile_sum += row[5]
        if row[2] != 0 or row[3] != 0 or row[4] != 0 or row[5] != 0:
            success_count += 1
        sql_file.write(str(row[0])+','+str(row[1])+','+str(row[2]) +
                    ','+str(row[3])+','+str(row[4])+','+str(row[5])+'\n')

    sql_file.write(str(total_id)+','+str(success_count)+','+str(AverageDataProfile_sum)+','+str(EnergyProfile_sum) +
                ','+str(EOBProfile_sum)+','+str(LoadProfile_sum)+'\n')
    sql_file.close()
    print('export csv successfully')
    print('--------------------------------')
    conn.close()


print('all done')

