import os
import re
import sqlite3

srcDir = './srcDir'
dstDir = './dstDir'

path = 'G:\\1log\\test\\log\\'
log_name = 'log'    #合并文件名


## 第一部分：日志处理
filelist = os.listdir(path)

filesum = 0 #message总数
startnum = 0    #重命名开始计数

#将单个后缀改名
for i in filelist:
    #判断是否为日志文件
    isMessage = re.match(r'messages.{0,2}', i)
    if isMessage:
        filesum +=1
    #判断文件名长度为10
    if len(i) == 10:
        messagename = 'messages.0%d' %startnum
        os.rename(path+i,path+messagename)
        # print(path+i,'-->',path+messagename)
        if startnum > 9:
            print("too many 10 length messages")
            os.exit()
        else:
            startnum +=1


#更新列表
filelist = os.listdir(path)
print('file count is ', filesum)
filesum -= 1

#倒序改名
for i in filelist:
    #判断是否为日志文件
    isMessage = re.match(r'messages.{0,2}', i)
    if isMessage:
        messagename = 'messages.%03d' %filesum
        # print(path+i,'-->',path+messagename)
        os.rename(path+i,path+messagename)
        filesum -= 1

#新建合并文件，二进制写入，行尾符不会自动识别系统，或者newline='\n'
all_log = open(path+log_name,'wb')
#更新列表
filelist = os.listdir(path)
#合并文件
for i in filelist:
    #判断是否为日志文件
    isMessage = re.match(r'messages.{0,2}', i)
    if isMessage:
        x = open(path+i, 'rb')
        all_log.write(x.read())
        x.close()
all_log.close()

#正则表达式
pattern_line = r'((.*SRWF -> 3_47,TX.*\n(.*DC: [0-9A-F]*)*\n)|(.*SRWF.*\n(.*DC: [0-9A-F]{0,}\n)*.*unpack3762.*\n))'
#转化为对象
pattern = re.compile(pattern_line)
all_log = open(path+log_name,'r',encoding='UTF-8')
result1 = pattern.findall(all_log.read())
all_log.close()
srwf_info = open(path+'3762.log','w',newline='\n',encoding='UTF-8')
#匹配的所有内容输出到文件
for i in result1:
    srwf_info.write(i[0])   #第一个捕获组，最外面的括号
    srwf_info.write('\n')

srwf_info.close()
#正则替换，删除RX后的一条记录
def change(value):
    return value.group(1)+'\n'

srwf_info = open(path+'3762.log','r',newline='\n',encoding='UTF-8')
result2 = re.sub(r'(.*SRWF -> 3_47,RX).*\n(.*\n)',change,srwf_info.read())
srwf_info.close()

srwf_info =  open(path+'3762.log','w',newline='\n',encoding='UTF-8')
srwf_info.write(result2)
srwf_info.close()

print('log processing success')
print('--------------------------------')
## 第二部分：数据库处理

conn = sqlite3.connect(path+'dc.db')
print('open dc.db successfully')
