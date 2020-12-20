import re
import pandas as pd
import os
from datetime import datetime, timedelta
import numpy as np
from influxdb import DataFrameClient
import sched, time
import glob

#вставка в инфлк
def to_influx(df,measurement,tags):
    df = df.reset_index()
    df.index = df['time']
    del df['time']
    client = DataFrameClient('tst-nt-cmis01', 8086, '', '', 'sar_ib')
    client.write_points(df, measurement, tag_columns = tags, protocol='json', batch_size=2048)
def parse_sar(filename):
    file = open(filename,'r')
    lines = file.readlines()
    #get server name and date
    pattern='\((.*)\)(?!$)?( .([0-9]{4}-[0-9]{2}-[0-9]{2}))'
    server_name = re.search(pattern,lines[0],re.IGNORECASE).group(1)
    date = re.search(pattern,lines[0],re.IGNORECASE).group(3)
    print(server_name,date)
    dict_metrics={'cpu':['CPU      %usr     %nice      %sys   %iowait    %steal      %irq     %soft    %guest    %gnice     %idle','CPU'],
                  'context_and_task':['proc/s   cswch/s',''],
                  'swap_paging_stats':['pswpin/s pswpout/s',''],
                  'paging_stats':['pgpgin/s pgpgout/s   fault/s  majflt/s  pgfree/s pgscank/s pgscand/s pgsteal/s    %vmeff',''],
                  'disk_tps':['tps      rtps      wtps   bread/s   bwrtn/s',''],
                  'memory_paging':['frmpg/s   bufpg/s   campg/s',''],
                  'memory_stats':['kbmemfree kbmemused  %memused kbbuffers  kbcached  kbcommit   %commit  kbactive   kbinact   kbdirty',''],
                  'swap_usage':['kbswpfree kbswpused  %swpused  kbswpcad   %swpcad',''],
                  'huge_pages':['kbhugfree kbhugused  %hugused',''],
                  'file_system_usage':['dentunusd   file-nr  inode-nr    pty-nr',''],
                  'server_tasks':['runq-sz  plist-sz   ldavg-1   ldavg-5  ldavg-15   blocked',''],
                  'tty_stats':['TTY   rcvin/s   xmtin/s framerr/s prtyerr/s     brk/s   ovrun/s',''],
                  'disk_io':['DEV       tps  rd_sec/s  wr_sec/s  avgrq-sz  avgqu-sz     await     svctm     %util','DEV'],
                  'net_stats':['IFACE   rxpck/s   txpck/s    rxkB/s    txkB/s   rxcmp/s   txcmp/s  rxmcst/s','IFACE'],
                  'net_error_stats':['IFACE   rxerr/s   txerr/s    coll/s  rxdrop/s  txdrop/s  txcarr/s  rxfram/s  rxfifo/s  txfifo/s','IFACE'],
                  'net_socket_stats':['totsck    tcpsck    udpsck    rawsck   ip-frag    tcp-tw','']
                  }
    i =0
    for dict_metric in dict_metrics:
        arr = []
        for i in range(len(lines)-1):
            if(dict_metrics.get(dict_metric)[0] in lines[i]  and not 'Average' in lines[i]):
                columns =lines[i].replace('\n','').split()
                columns[0] = 'Time'
                columns[1] = 'AmPm'
                i=i+1
                while(lines[i]!='\n'  and not 'Average:' in lines[i]):
                    line = lines[i].replace('\n','')
                    line = line.split()
                    dict1=dict(zip(columns,line))
                    arr.append(dict1)
                    i = i + 1
        if arr!=[]:
            df = pd.DataFrame(arr)
            df['time']=date+' '+df['Time']+' '+df['AmPm']
            df['time'] =pd.to_datetime(df['time'], format='%Y-%m-%d %I:%M:%S %p')
            df = df.drop(columns=['AmPm','Time'])
            tags = []
            if dict_metrics.get(dict_metric)[1] != '':
                tags = [dict_metrics.get(dict_metric)[1]]
                for column in columns:
                    for tag in tags:
                        if tag != column and column != 'Time' and column != 'AmPm':
                            df = df.astype({column:float})
            to_influx(df,dict_metric,tags)


parse_sar('sar05')
