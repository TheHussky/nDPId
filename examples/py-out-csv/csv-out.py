#!/usr/bin/env python3

import os
import sys
import pandas as pd


sys.path.append(os.path.dirname(sys.argv[0]) + '/../../dependencies')
sys.path.append(os.path.dirname(sys.argv[0]) + '/../share/nDPId')
sys.path.append(os.path.dirname(sys.argv[0]))
sys.path.append(sys.base_prefix + '/share/nDPId')
import nDPIsrvd
from nDPIsrvd import nDPIsrvdSocket, TermColor

def onJsonLineRecvdToCSV(fd, json_dict, instance, current_flow, global_user_data):
    df = pd.read_json(json_dict)
    df.to_csv(fd, mode="a")
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
    with open("out.csv", 'a') as fd:
        nsock.loop(fd, onJsonLineRecvdToCSV, None, None)
