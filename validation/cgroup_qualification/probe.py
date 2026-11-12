"""Ephemeral hosted VM qualification only. No child creation or workload launch."""
import ctypes
import errno
import json
import os
from pathlib import Path
import platform
import re
import sys


def require(ok):
    if not ok:raise RuntimeError('qualification prerequisite rejected')


def main():
    require(os.geteuid()==0 and platform.machine()=='x86_64')
    uid=int(sys.argv[1]);gid=int(sys.argv[2]);require(uid>0 and gid>0)
    entry=Path('/proc/self/cgroup').read_text().strip()
    require(entry.startswith('0::/'))
    relative=entry[3:]
    require(re.fullmatch(r'/system.slice/opencompute-qual-[0-9]+-[0-9]+\.service',relative) is not None)
    root=Path('/sys/fs/cgroup'+relative)
    require((root/'cgroup.type').read_text().strip()=='domain')
    require((root/'cgroup.procs').read_text().split()==[str(os.getpid())])
    require({'cpu','memory','pids'} <= set((root/'cgroup.controllers').read_text().split()))
    require((root/'memory.max').read_text().strip()=='134217728')
    require((root/'memory.swap.max').read_text().strip()=='0')
    require((root/'pids.max').read_text().strip()=='16')
    quota,period=map(int,(root/'cpu.max').read_text().split());require(quota*4<=period)
    print(json.dumps({'bounded_service':True,'controllers':['cpu','memory','pids']}),flush=True)
    manager=root/'manager';leaf=root/'probe';moved=False;enabled=False
    try:
        manager.mkdir();(manager/'cgroup.procs').write_text(str(os.getpid()));moved=True
        (root/'cgroup.subtree_control').write_text('+cpu +memory +pids');enabled=True
        leaf.mkdir()
        for name,value in [('cpu.max','25000 100000'),('memory.max','67108864'),('memory.swap.max','0'),('pids.max','4')]:
            (leaf/name).write_text(value);require((leaf/name).read_text().strip()==value)
        require((leaf/'cgroup.procs').read_text().strip()=='')
        require('populated 0' in (leaf/'cgroup.events').read_text().splitlines())
        require((leaf/'cgroup.kill').exists())
        # Empty leaf delegation only, to the existing temporary runner identity.
        for path in (leaf,leaf/'cgroup.procs',leaf/'cgroup.threads',leaf/'cgroup.subtree_control'):
            os.chown(path,uid,gid)
        try:
            os.setegid(gid);os.seteuid(uid)
            fd=os.open(leaf/'cgroup.procs',os.O_WRONLY|os.O_NOFOLLOW);os.close(fd)
            writable=os.access(leaf,os.W_OK,effective_ids=True)
        finally:os.seteuid(0);os.setegid(0)
        require(writable)
        # NULL and size zero MUST fail; never creates a process. x86_64 syscall 435.
        libc=ctypes.CDLL(None,use_errno=True);ctypes.set_errno(0)
        result=libc.syscall(ctypes.c_long(435),ctypes.c_void_p(0),ctypes.c_size_t(0))
        code=ctypes.get_errno()
        require(result==-1)
        print(json.dumps({'empty_leaf_delegation':True,'clone3_invalid_probe_errno':code,
            'clone3_handler_visible':code==errno.EINVAL,'atomic_entry_tested':False,
            'launcher_verified':False,'gpu_stop_verified':False}),flush=True)
        require(code==errno.EINVAL)
    finally:
        # Only our empty leaf/manager are removed; systemd owns parent lifecycle.
        if leaf.exists():leaf.rmdir()
        if enabled:(root/'cgroup.subtree_control').write_text('-cpu -memory -pids')
        if moved:(root/'cgroup.procs').write_text(str(os.getpid()))
        if manager.exists():manager.rmdir()
    print('qualification_checks_complete_no_workload_started',flush=True)

if __name__=='__main__':
    try:main()
    except Exception:
        print('qualification_failed_closed',flush=True)
        sys.exit(1)
