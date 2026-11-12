/* Review candidate only. One thread, one clone3, no exec in child, no migration. */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <linux/sched.h>
#include <poll.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

static volatile sig_atomic_t expired;
static void expire(int sig) { (void)sig; expired=1; }
static int readtext(const char *path, char *out, size_t cap) {
    int fd=open(path,O_RDONLY|O_CLOEXEC|O_NOFOLLOW);
    if(fd<0)return -1;
    ssize_t n=read(fd,out,cap-1); char extra;
    int ok=n>0 && read(fd,&extra,1)==0;
    close(fd); if(!ok)return -1;out[n]=0;return 0;
}
static int transfer(int fd, void *buffer, size_t n, int writing) {
    size_t done=0;
    while(done<n && !expired) {
        ssize_t k=writing?write(fd,(char *)buffer+done,n-done):read(fd,(char *)buffer+done,n-done);
        if(k<=0)return -1;
        done+=(size_t)k;
    }
    return done==n?0:-1;
}
int main(int argc,char **argv) {
    /* Fixed inherited FDs from reviewed wrapper: 3 directory, 4 kill control.
       argv supplies only the expected proc cgroup record, never printed. */
    if(argc!=4 || strlen(argv[1])>=1024)return 2;
    char *end1,*end2;long source_dir=strtol(argv[2],&end1,10),source_kill=strtol(argv[3],&end2,10);
    if(*end1||*end2||source_dir<3||source_kill<3||source_dir==source_kill)return 2;
    /* Duplicate high first so mapping to 3/4 cannot overwrite the other input. */
    int d=fcntl((int)source_dir,F_DUPFD_CLOEXEC,100),k=fcntl((int)source_kill,F_DUPFD_CLOEXEC,100);
    if(d<0||k<0)return 2;
    if(dup2(d,3)<0||dup2(k,4)<0)return 2;
    close(d);close(k);
    if(source_dir!=3 && source_dir!=4)close((int)source_dir);
    if(source_kill!=3 && source_kill!=4)close((int)source_kill);
    struct sigaction sa={0};sa.sa_handler=expire;sigemptyset(&sa.sa_mask);
    if(sigaction(SIGALRM,&sa,NULL)<0)return 2;
    signal(SIGPIPE,SIG_IGN);alarm(5);
    int report[2],gate[2];
    if(pipe2(report,O_CLOEXEC)<0)return 2;
    if(pipe2(gate,O_CLOEXEC)<0){close(report[0]);close(report[1]);return 2;}
    int pidfd=-1;pid_t child=-1;int ok=0,status=0;
    struct clone_args args={0};
    args.flags=CLONE_INTO_CGROUP|CLONE_PIDFD;
    args.cgroup=3;args.pidfd=(uintptr_t)&pidfd;args.exit_signal=SIGCHLD;
    child=(pid_t)syscall(SYS_clone3,&args,sizeof(args)); /* exactly one attempt */
    if(child==0) {
        /* Same root identity, trusted no-compute fixture ONLY; no isolation claim. */
        close(3);close(4);close(report[0]);close(gate[1]);
        char record[1024]={0},release=0;
        if(readtext("/proc/self/cgroup",record,sizeof(record))<0 ||
           transfer(report[1],record,sizeof(record),1)<0 ||
           transfer(gate[0],&release,1,0)<0 || release!='X')_exit(3);
        close(report[1]);close(gate[0]);_exit(0);
    }
    close(report[1]);close(gate[0]);
    if(child<0){puts("clone3_rejected_no_retry");goto cleanup;}
    if(pidfd<0)goto cleanup;
    char reported[1024],observed[1024],path[64],members[1024],expected[64];
    if(transfer(report[0],reported,sizeof(reported),0)<0)goto cleanup;
    if(!memchr(reported,0,sizeof(reported)) || strcmp(reported,argv[1]))goto cleanup;
    snprintf(path,sizeof(path),"/proc/%ld/cgroup",(long)child);
    if(readtext(path,observed,sizeof(observed))<0 || strcmp(observed,argv[1]))goto cleanup;
    int fd=openat(3,"cgroup.procs",O_RDONLY|O_CLOEXEC|O_NOFOLLOW);
    if(fd<0)goto cleanup;
    ssize_t n=read(fd,members,sizeof(members)-1);close(fd);
    snprintf(expected,sizeof(expected),"%ld\n",(long)child);
    if(n<=0 || (size_t)n>=sizeof(members)-1)goto cleanup;
    members[n]=0;if(strcmp(members,expected))goto cleanup;
    puts("child_report_parent_proc_and_membership_match");
    char release='X';if(transfer(gate[1],&release,1,1)<0)goto cleanup;
    struct pollfd p={.fd=pidfd,.events=POLLIN};
    if(poll(&p,1,2000)!=1 || !(p.revents&POLLIN))goto cleanup;
    if(waitpid(child,&status,0)!=child)goto cleanup;
    child=-1;
    if(!WIFEXITED(status)||WEXITSTATUS(status)!=0||expired)goto cleanup;
    ok=1;
cleanup:
    close(report[0]);close(gate[1]);
    if(child>0) {
        /* Bound scope, never a bare PID signal. Outer service timeout is fallback. */
        if(lseek(4,0,SEEK_SET)<0 || write(4,"1",1)!=1)ok=0;
        if(pidfd>=0) {
            struct pollfd p={.fd=pidfd,.events=POLLIN};
            if(poll(&p,1,2000)==1 && (p.revents&POLLIN)) {
                if(waitpid(child,&status,0)!=child)ok=0;
            } else ok=0;
        }
    }
    if(pidfd>=0)close(pidfd);
    close(3);close(4);alarm(0);
    puts(ok?"ordinary_child_reaped":"atomic_entry_failed_closed");
    return ok?0:1;
}
