# -*- coding:utf-8 -*-
import os
import sys
import re
import sqlite3
import time
import configparser


def datetime_timestamp(dt):
    time.strptime(dt, '%Y-%m-%d %H:%M:%S')
    s = time.mktime(time.strptime(dt, '%Y-%m-%d %H:%M:%S'))
    return int(s)

def timestamp_datetime(value):
    format = '%Y-%m-%d %H:%M:%S'
    value = time.localtime(value)
    dt = time.strftime(format, value)
    return dt

# 读入config.ini文件
config = configparser.ConfigParser()
os.chdir(sys.path[0])
config_file = os.path.abspath(os.path.join(os.getcwd(), 'config.ini'))
config.read(config_file, encoding='UTF-8')


start_time = datetime_timestamp(
    config['db']['start_time'])      # 数据库分析开始时间，运行程序的当地时间
end_time = datetime_timestamp(
    config['db']['end_time'])        # 数据库分析结束时间，运行程序的当地时间
start_EOB_time = datetime_timestamp(
    config['db']['start_EOB_time'])  # 数据库分析开始时间，运行程序的当地时间
end_EOB_time = datetime_timestamp(
    config['db']['end_EOB_time'])    # 数据库分析开始时间，运行程序的当地时间
start_load_time = datetime_timestamp(
    config['db']['start_load_time'])  # 数据库分析开始时间，运行程序的当地时间
end_load_time = datetime_timestamp(
    config['db']['end_load_time'])    # 数据库分析开始时间，运行程序的当地时间
# 日志开始分析时间
log_start_time = config['log']['log_start_time']
# 日志结束分析时间
log_end_time = config['log']['log_end_time']

time_type = config['db']['time_type']  # 数据库导出时间

path = config['config']['path']  # 日志父路径
dir_name = os.path.split(path)[1]  # 日志文件夹名
# out_dir = path+ '\\output\\'
out_dir = config['config']['out_dir']
if out_dir == '':
    out_dir = os.path.join(path, 'output')

auto_search = config.getint('config', 'auto_search')     # 自动查找path路径下日志文件夹
change_name = config.getint('config', 'change_name')      # 改名合并(已经能自动判断)
force_merge = config.getint(
    'config', 'force_merge')     # 强制合并,不判断文件是否存在(需开启改名)
log_date_range = config.getint('config', 'log_date_range')  # 启用日志范围(暂不支持跨月)
log_analy = config.getint('config', 'log_analy')        # 日志分析(需先改名合并)
once_analy = config.getint('config', 'once_analy')      # 一次抄表成功率分析(需先改名合并)
log_profile = config.getint('config', 'log_profile')      # 一次抄表成功率分析(需先改名合并)
db_analy = config.getint('config', 'db_analy')        # 数据库分析
EOB_analy = config.getint('config', 'EOB_analy')       # EOB分析
load_analy = config.getint('config', 'load_analy')       # loadprofile分析
# True: 1.7.2日志 , False: 1.7.2之后版本日志
old_version = config.getint('config', 'old_version')
print_type = config.getint('config', 'print_type')

# start_time = datetime_timestamp('2020-05-13 05:00:00')      # 数据库分析开始时间，运行程序的当地时间
# end_time = datetime_timestamp('2020-05-14 05:00:00')        # 数据库分析结束时间，运行程序的当地时间
# start_EOB_time = datetime_timestamp('2020-05-01 05:00:00')  # 数据库分析开始时间，运行程序的当地时间
# end_EOB_time = datetime_timestamp('2020-05-14 05:00:00')    # 数据库分析开始时间，运行程序的当地时间
# log_start_time = 'May 13 18:00:00'                          # 日志开始分析时间
# log_end_time = 'May 13 19:00:00'                            # 日志结束分析时间

# time_type = 'data_time'  # 数据库导出时间

# path = r'G:\沙特现场问题'  # 日志父路径
# dir_name = '台区5-集中器日志-SXE2030180003822-0513-升级前'  # 日志文件夹名
# # out_dir = path+ '\\output\\'
# out_dir = os.path.join(path, dir_name) + '\\output\\'

# auto_search = 0     # 自动查找path路径下日志文件夹
# change_name = 1     # 改名合并(已经能自动判断)
# force_merge = 1     # 强制合并,不判断文件是否存在(需开启改名)
# log_date_range = 1  # 启用日志范围(暂不支持跨月)
# log_analy = 0       # 日志分析(需先改名合并)
# once_analy = 0      # 一次抄表成功率分析(需先改名合并)
# db_analy = 0        # 数据库分析
# EOB_analy = 0       # EOB分析()
# old_version = 0     # True: 1.7.2日志 , False: 1.7.2之后版本日志


rootpath = path
pathlist = []
if auto_search == 1:
    pathlist = os.listdir(rootpath)
else:
    pathlist.append(path)
for sub_dir in pathlist:
    if auto_search == 1:
        path = os.path.join(rootpath, sub_dir)
    if os.path.isdir(path) and sub_dir != 'output':
        log_name = 'all.log'  # 日志合并文件文件名
        dir_name = os.path.split(path)[1] 
        profile_name = dir_name+'_'+time_type+'_' + \
            str(start_time)+'-'+str(end_time)+'.csv'  # 数据库导出文件名
        srwf_name = dir_name+'_3762.log'   # 3762报文文件名
        EOB_name = dir_name+'_EOB_'+str(start_EOB_time)+'_'+str(end_EOB_time)+'.csv'
        load_name = dir_name+'_loadprofile_'+str(start_EOB_time)+'_'+str(end_EOB_time)+'.csv'
        profile_log_name =  dir_name+'_saveProfile_'   # saveProfile日志导出文件名
        print_log_name = dir_name+'_logout.log'

        if not os.path.exists(out_dir):
            os.makedirs(out_dir)

        if print_type != 0:
            print_type_temp = sys.stdout
            print_log = open(os.path.join(out_dir,print_log_name),'w')
            sys.stdout = print_log

        print('START ANALYSIS: '+path)
        print('--------------------------------')

        # 第一部分：日志处理
        # 改名
        filelist = os.listdir(path)

        filesum = 0  # message总数
        startnum = 0  # 重命名开始计数

        if change_name == True:
            if os.path.isfile(os.path.join(path, log_name)):
                print('\'{0}\' already exist, escape change name.'.format(log_name))
                if force_merge == False:
                    print('To merge log file again, please enable force_merge.')
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
                                os.rename(os.path.join(path, i),
                                        os.path.join(path, messagename))
                                # print(path+i,'-->',path+messagename)
                                if startnum > 9:
                                    print("too many 10 length messages")
                                    os.exit()
                                else:
                                    startnum += 1

                    # 更新列表
                    filelist = os.listdir(path)
                    print('file count is '+ str(filesum))
                    filesum -= 1

                    # 倒序改名
                    for i in filelist:
                        # 判断是否为日志文件
                        isMessage = re.match(r'messages.{0,2}', i)
                        if isMessage:
                            messagename = 'messages.%03d' % filesum
                            # print(path+i,'-->',path+messagename)
                            os.rename(os.path.join(path, i),
                                    os.path.join(path, messagename))
                            filesum -= 1
                    print('change name success')

            # 合并文件
            if not os.path.isfile(os.path.join(path, log_name)) or force_merge:
                # 新建合并文件，二进制写入，行尾符不会自动识别系统，或者newline='\n'
                all_log = open(os.path.join(path, log_name), 'wb')
                # 更新列表
                filelist = os.listdir(path)
                # 合并文件
                for i in filelist:
                    # 判断是否为日志文件
                    isMessage = re.match(r'messages.{0,2}', i)
                    if isMessage:
                        x = open(os.path.join(path, i), 'rb')
                        all_log.write(x.read())
                        x.close()
                all_log.close()

                print('merge file success')
                print('--------------------------------')

        # 日志时间范围
        if log_date_range == True:
            # 判断日期是否合法
            if log_end_time <= log_start_time:
                print('illegal parameter, log_end_time({0}) should be greater than log_start_time{1}, please check them.'.format(
                    log_end_time, log_start_time))
            else:
                if not os.path.isfile(os.path.join(path, log_name)):
                    print('{0} not exist, please change name first'.format(log_name))
                    print('--------------------------------')
                else:
                    # 正则表达式
                    pattern_line = r'((.*) DC: .*)'
                    # 转化为对象
                    pattern = re.compile(pattern_line)
                    all_log = open(os.path.join(path, log_name), 'r', encoding='UTF-8', errors='ignore')
                    result1 = pattern.findall(all_log.read())
                    all_log.close()
                    srwf_info = open(os.path.join(path, log_name), 'w',
                                    newline='\n', encoding='UTF-8')
                    # 匹配的所有内容输出到文件
                    for i in result1:
                        if i[1] > log_start_time and i[1] < log_end_time:
                            srwf_info.write(i[0])  # 第一个捕获组，最外面的括号
                            srwf_info.write('\n')

                    srwf_info.close()

        # 日志分析功能
        if log_analy == True:
            if not os.path.isfile(os.path.join(path, log_name)):
                print('{0} not exist, please change name first'.format(log_name))
                print('--------------------------------')
            else:
                # 正则表达式
                pattern_line = r'((.*SRWF -> 3_47,TX.*\n(.*DC: [0-9A-F]*)*\n)|(.*SRWF.*\n(.*DC: [0-9A-F]{0,}\n)*.*unpack3762.*\n))'
                # 转化为对象
                pattern = re.compile(pattern_line)
                all_log = open(os.path.join(path, log_name), 'r', encoding='UTF-8', errors='ignore')
                result1 = pattern.findall(all_log.read())
                all_log.close()
                srwf_info = open(os.path.join(out_dir, srwf_name), 'w',
                                newline='\n', encoding='UTF-8')
                # 匹配的所有内容输出到文件
                for i in result1:
                    srwf_info.write(i[0])  # 第一个捕获组，最外面的括号
                    srwf_info.write('\n')

                srwf_info.close()

                if old_version == True:
                    # 正则替换，删除RX后的一条记录
                    def change(value):
                        return value.group(1)+'\n'
                    srwf_info = open(os.path.join(out_dir, srwf_name), 'r',
                                    newline='\n', encoding='UTF-8')
                    result2 = re.sub(r'(.*SRWF -> 3_47,RX).*\n(.*\n)',
                                    change, srwf_info.read())
                    # result2 = srwf_info.read()
                    srwf_info.close()

                    srwf_info = open(os.path.join(out_dir, srwf_name), 'w',
                                    newline='\n', encoding='UTF-8')
                    srwf_info.write(result2)
                    srwf_info.close()

                print('3762log processing success')
                print('--------------------------------')

        # 一次抄表成功率分析
        if once_analy == True:
            if not os.path.isfile(os.path.join(out_dir, srwf_name)):
                print('{0} not exist, please change name first'.format(srwf_name))
                print('--------------------------------')
            else:
                # 统计一次抄表成功率
                # 正则表达式
                pattern_line = r'AFN : 13, FN : 1'
                # 转化为对象
                pattern = re.compile(pattern_line)
                srwf_info = open(os.path.join(out_dir, srwf_name), 'r', encoding='UTF-8', errors='ignore')
                result1 = pattern.findall(srwf_info.read())
                current_count = len(result1)

                pattern_line = r'13010000000000|13010000000002'
                # 转化为对象
                pattern = re.compile(pattern_line)
                srwf_info = open(os.path.join(out_dir, srwf_name), 'r', encoding='UTF-8', errors='ignore')
                result1 = pattern.findall(srwf_info.read())
                wrong_count = len(result1)
                print("wrong_count:{0}".format(wrong_count))
                print("total_count:{0}".format(current_count))
                print("success_rate:{:.2f}%".format(
                    (current_count-wrong_count)/current_count*100))
                print('--------------------------------')

        # 分析saveProfile
        if log_profile == True:
            # 正则表达式
            pattern_line = r'(.*saveProfile,taskType:([0-9]*).*)'
            # 转化为对象
            pattern = re.compile(pattern_line)
            all_log = open(os.path.join(path, log_name), 'r', encoding='UTF-8', errors='ignore')
            result1 = pattern.findall(all_log.read())
            all_log.close()
            profileId_list = []
            if len(result1) != 0:
                # 将所有profile id导入到list中
                for i in result1:
                    profileId_list.append(i[1])
                # 去重
                profileId_list = list(set(profileId_list))
                # 将日志分别写到对应文件中
                for i in profileId_list:
                    print('profile_id: '+i,end='')
                    # save_profile的数量
                    saveprofile_sum = 0
                    temp_profile_file = open(os.path.join(out_dir, profile_log_name+i+'.log'), 'w',
                                    newline='\n', encoding='UTF-8')
                    for j in result1:
                        if j[1] == i: 
                            temp_profile_file.write(j[0])  # 第一个捕获组，最外面的括号
                            temp_profile_file.write('\n')
                            saveprofile_sum += 1
                    temp_profile_file.close()
                    # os.rename(os.path.join(out_dir, profile_log_name+i+'.log'),os.path.join(out_dir, profile_log_name+i+'_'+str(saveprofile_sum)+'.log'))
                    print(', sum: '+str(saveprofile_sum))
                    saveprofile_sum = 0
            else:
                print('no saveProfile in this log')
            print('--------------------------------')
        # 第二部分：数据库处理
        if db_analy == True:
            conn = sqlite3.connect(os.path.join(path, 'dc.db'))
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
            # print(sql_command)
            cursor = sql_c.execute(
                sql_command
            )
            sql_file = open(os.path.join(out_dir, profile_name), 'w')
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
            print('export profile csv successfully')
            print('--------------------------------')
            conn.close()
        # EOB 分析
        if EOB_analy == True:
            conn = sqlite3.connect(os.path.join(path, 'dc.db'))
            print('open dc.db successfully')
            sql_c = conn.cursor()
            sql_command = '''
                SELECT
                    meter.id,
                    serial_no,
                    data_time,
                    save_time 
                FROM
                    meter,
                    data_EOB_DataProfile_sd 
                WHERE
                    data_EOB_DataProfile_sd.meter_id = meter.id 
                    AND save_time > {0} 
                    AND save_time < {1} 
                ORDER BY
                    save_time;
            '''.format(start_EOB_time, end_EOB_time)  # 格式化sql语句
            # print(sql_command)
            cursor = sql_c.execute(sql_command)
            sql_file = open(os.path.join(out_dir, EOB_name), 'w')
            sql_file.write(
                'id,serial_no,data_time,save_time\n')

            total_id = 0  # 表的总数量

            for row in cursor:
                total_id += 1

                sql_file.write(str(row[0])+','+str(row[1])+','+timestamp_datetime(row[2]) +
                            ','+timestamp_datetime(row[3])+'\n')

            sql_file.close()
            print('export EOB csv successfully')
            print('--------------------------------')
            conn.close()
        # loadprofile分析
        if load_analy == True:
            conn = sqlite3.connect(os.path.join(path, 'dc.db'))
            print('open dc.db successfully')
            sql_c = conn.cursor()
            sql_command = '''
                SELECT
                    meter.id,
                    serial_no,
                    data_time,
                    save_time 
                FROM
                    meter,
                    data_LoadProfile_sd 
                WHERE
                    data_LoadProfile_sd.meter_id = meter.id 
                    AND save_time > {0} 
                    AND save_time < {1} 
                ORDER BY
                    save_time;
            '''.format(start_load_time, end_load_time)  # 格式化sql语句
            # print(sql_command)
            cursor = sql_c.execute(sql_command)
            sql_file = open(os.path.join(out_dir, load_name), 'w')
            sql_file.write(
                'id,serial_no,data_time,save_time\n')

            total_id = 0  # 表的总数量

            for row in cursor:
                total_id += 1

                sql_file.write(str(row[0])+','+str(row[1])+','+timestamp_datetime(row[2]) +
                            ','+timestamp_datetime(row[3])+'\n')

            sql_file.close()
            print('export load profile csv successfully')
            print('--------------------------------')
            conn.close()

        print('ALL DONE')
        print('--------------------------------')

        if print_type != 0:
            sys.stdout = print_type_temp
            print_log.close()
            print_log = open(os.path.join(out_dir,print_log_name),'r')
            print(print_log.read())
            print_log.close()