"""Unexecuted review candidate: one fixed native no-compute child."""
import subprocess
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
    binary=Path(sys.argv[1]);require(binary.is_absolute())
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
        directory=os.open(leaf,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        kill=os.open(leaf/'cgroup.kill',os.O_WRONLY|os.O_NOFOLLOW)
        try:
            # No preexec_fn in Python. Native wrapper receives descriptors via
            # pass_fds, and maps them itself before its only clone3 call.
            result=subprocess.run([str(binary), '0::'+relative+'/probe\n',
                str(directory),str(kill)],pass_fds=(directory,kill),
                env={},timeout=10,check=False)
            require(result.returncode==0)
            require((leaf/'cgroup.procs').read_text().strip()=='')
            require('populated 0' in (leaf/'cgroup.events').read_text().splitlines())
            print('reaped_and_empty_same_domain',flush=True)
        finally:
            os.close(kill);os.close(directory)
    finally:
        # Only our empty leaf/manager are removed; systemd owns parent lifecycle.
        if leaf.exists():
            if 'populated 0' not in (leaf/'cgroup.events').read_text().splitlines():
                (leaf/'cgroup.kill').write_text('1')
                raise RuntimeError('scope retained for service cleanup')
            leaf.rmdir()
        if enabled:(root/'cgroup.subtree_control').write_text('-cpu -memory -pids')
        if moved:(root/'cgroup.procs').write_text(str(os.getpid()))
        if manager.exists():manager.rmdir()
    print('atomic_entry_probe_complete_not_launcher_or_gpu_validation',flush=True)

if __name__=='__main__':
    try:main()
    except Exception:
        print('qualification_failed_closed',flush=True)
        sys.exit(1)
