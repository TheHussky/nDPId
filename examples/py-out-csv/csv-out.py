#!/usr/bin/env python3

import os
import sys
import pandas as pd

out_fd = open("out.csv", 'a')

sys.path.append(os.path.dirname(sys.argv[0]) + '/../../dependencies')
sys.path.append(os.path.dirname(sys.argv[0]) + '/../share/nDPId')
sys.path.append(os.path.dirname(sys.argv[0]))
sys.path.append(sys.base_prefix + '/share/nDPId')
import nDPIsrvd
from nDPIsrvd import nDPIsrvdSocket, TermColor

class MutableDataframes:
    def __init__(self):
        self.FlowDF = [pd.DataFrame()]
        self.PacketDF = [pd.DataFrame()]


def onJsonLineRecvdToCSV(json_dict, instance, current_flow, global_user_data):
    new_df = pd.DataFrame([json_dict])
    if 'flow_event_name' in json_dict:
        if json_dict['flow_event_name'] == 'analyse' and json_dict['flow_state'] == 'finished':
            global_user_data.FlowDF[0] = pd.concat([global_user_data.FlowDF[0], new_df], ignore_index=False)
        #global_user_data.FlowDF[0] = global_user_data.FlowDF[0].join(new_df, how='outer', on='flow_id', lsuffix="initial")

    if 'packet_event_name' in json_dict:
        global_user_data.PacketDF[0] = pd.concat([global_user_data.PacketDF[0], new_df], ignore_index=True)
    print(global_user_data.PacketDF[0])
    print(global_user_data.FlowDF[0])
    return True



if __name__ == '__main__':


    argparser = nDPIsrvd.defaultArgumentParser('Plain and simple nDPIsrvd json to csv event printer with filter capabilities.', True)
    args = argparser.parse_args()
    address = nDPIsrvd.validateAddress(args)

    sys.stderr.write('Recv buffer size: {}\n'.format(nDPIsrvd.NETWORK_BUFFER_MAX_SIZE))
    sys.stderr.write('Connecting to {} ..\n'.format(address[0]+':'+str(address[1]) if type(address) is tuple else address))

    nsock = nDPIsrvdSocket()
    nDPIsrvd.prepareJsonFilter(args, nsock)
    nsock.connect(address)
    df = MutableDataframes()
    try:
        nsock.loop(onJsonLineRecvdToCSV, None, df)
    except KeyboardInterrupt:
        # We want to save dfs either way
        pass
    
    df.FlowDF[0].to_csv('out_flows.csv', mode='w')
    df.PacketDF[0].to_csv('out_packets.csv', mode='w')
