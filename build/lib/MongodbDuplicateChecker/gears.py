import os
import time


def tell_the_datetime(time_stamp=None, compact_mode=False):
    if isinstance(time_stamp, str):
        time_stamp = int(time_stamp) if time_stamp and time_stamp.isdigit() else time.time()
    format_str = '%Y-%m-%d-%H-%M-%S' if compact_mode else '%Y-%m-%d %H:%M:%S'
    tm = time.strftime(format_str, time.localtime(time_stamp))
    return tm


def printer(msg=None, length_ctrl=True, fill_with='-', alignment='l', msg_head_tail=None, print_out=True, reflash=False):
    fill_with = '-' if not fill_with.strip() else fill_with.strip()
    try:
        length = os.get_terminal_size().columns
    except:
        length = 150
    al = {'l': '<', 'r': '>', 'm': '^'}.get(alignment)
    if not msg_head_tail and alignment == 'l':
        msg_head_tail = [' >>> ', '']
    elif not msg_head_tail:
        msg_head_tail = ['', '']
    if isinstance(msg_head_tail, str):
        msg_head_tail = [msg_head_tail, '']
    elif not isinstance(msg_head_tail, list):
        try:
            msg_head_tail = list(msg_head_tail)
        except:
            pass
    msg_head_tail = [' {} '.format(x) if x else '' for x in msg_head_tail]
    if msg is not None:
        if not isinstance(msg, str):
            msg = str(msg)
        if length_ctrl:
            if isinstance(length_ctrl, int) and length_ctrl > 1:
                length = length_ctrl
            if len(msg) > length:
                msg = f" {msg[:(length - 10)]} ..."
            else:
                msg = f" {msg} "
        msg = msg_head_tail[0] + msg + msg_head_tail[1]
    else:
        msg = fill_with
    msg = ("{:%s%s%s}" % (fill_with, al, length)).format(msg)
    if print_out:
        if not reflash:
            print(msg)
        else:
            print(f"{msg}\r", end='')
    else:
        return msg
