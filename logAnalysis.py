import os
import re

srcDir = './srcDir'
dstDir = './dstDir'

path = 'G:\\1log\\test\\log\\'

filelist = os.listdir(path)

filesum = 0 #message总数
startnum = 0    #重命名开始计数

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
for i in filelist:
    #判断是否为日志文件
    isMessage = re.match(r'messages.{0,2}', i)
    if isMessage:
        messagename = 'messages.%03d' %filesum
        # print(path+i,'-->',path+messagename)
        os.rename(path+i,path+messagename)
        filesum -= 1

filelist = os.listdir(path)
for i in filelist:
    #判断是否为日志文件
    isMessage = re.match(r'messages.{0,2}', i)
    if isMessage:
        messagename = 'messages.%03d' %filesum
        # print(path+i,'-->',path+messagename)
        os.rename(path+i,path+messagename)
        filesum -= 1