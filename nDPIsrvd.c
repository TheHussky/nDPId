#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <linux/un.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <syslog.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

enum ev_type { JSON_SOCK, SERV_SOCK };

struct event {
    enum ev_type type;
    union {
        struct {
            int json_sockfd;
            struct sockaddr_un peer;
        } event_json;
        struct {
            int serv_sockfd;
            struct sockaddr_in peer;
        } event_serv;
    };
};

static char json_sockpath[UNIX_PATH_MAX] = "/tmp/ndpid-collector.sock";
static char serv_listen_addr[INET6_ADDRSTRLEN] = "127.0.0.1";
static uint16_t serv_listen_port = 7000;
static int json_sockfd;
static int serv_sockfd;

static int create_listen_sockets(void)
{
    json_sockfd = socket(AF_UNIX, SOCK_STREAM, 0);
    serv_sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (json_sockfd < 0 || serv_sockfd < 0)
    {
        syslog(LOG_DAEMON | LOG_ERR, "Error opening socket: %s", strerror(errno));
        return 1;
    }

    // This helps avoid spurious EADDRINUSE when the previous instance of this
    // server died.
    int opt = 1;
    if (setsockopt(json_sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0 ||
        setsockopt(serv_sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0)
    {
        syslog(LOG_DAEMON | LOG_ERR, "setsockopt with SO_REUSEADDR failed: %s", strerror(errno));
        return 1;
    }

    struct sockaddr_un json_addr;
    json_addr.sun_family = AF_UNIX;
    if (snprintf(json_addr.sun_path, sizeof(json_addr.sun_path), "%s", json_sockpath) <= 0)
    {
        syslog(LOG_DAEMON | LOG_ERR, "snprintf failed: %s", strerror(errno));
        return 1;
    }

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(serv_listen_port);
    if (inet_ntop(AF_INET, &serv_addr.sin_addr, &serv_listen_addr[0], INET_ADDRSTRLEN) == NULL)
    {
        syslog(LOG_DAEMON | LOG_ERR, "Error converting an internet address: %s", strerror(errno));
        return 1;
    }

    if (bind(json_sockfd, (struct sockaddr *)&json_addr, sizeof(json_addr)) < 0)
    {
        syslog(LOG_DAEMON | LOG_ERR, "Error on binding a JSON socket: %s", strerror(errno));
        return 1;
    }

    if (bind(serv_sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
    {
        syslog(LOG_DAEMON | LOG_ERR, "Error on binding the INET socket: %s", strerror(errno));
        return 1;
    }

    if (listen(json_sockfd, 16) < 0 || listen(serv_sockfd, 16) < 0)
    {
        syslog(LOG_DAEMON | LOG_ERR, "Error on listen: %s", strerror(errno));
        return 1;
    }

    int json_flags = fcntl(json_sockfd, F_GETFL, 0);
    int serv_flags = fcntl(serv_sockfd, F_GETFL, 0);
    if (json_flags == -1 || serv_flags == -1)
    {
        syslog(LOG_DAEMON | LOG_ERR, "Error getting fd flags: %s", strerror(errno));
        return 1;
    }
    if (fcntl(json_sockfd, F_SETFL, json_flags | O_NONBLOCK) == -1 ||
        fcntl(serv_sockfd, F_SETFL, serv_flags | O_NONBLOCK) == -1)
    {
        syslog(LOG_DAEMON | LOG_ERR, "Error setting fd flags: %s", strerror(errno));
        return 1;
    }

    return 0;
}

int main(void)
{
    openlog("nDPIsrvd", LOG_CONS | LOG_PERROR, LOG_DAEMON);

    if (create_listen_sockets() != 0)
    {
        return 1;
    }

    int epollfd = epoll_create1(0);
    if (epollfd < 0)
    {
        syslog(LOG_DAEMON | LOG_ERR, "Error creating epoll: %s", strerror(errno));
        return 1;
    }

    struct epoll_event accept_event;
    accept_event.data.fd = json_sockfd;
    accept_event.events = EPOLLIN;
    if (epoll_ctl(epollfd, EPOLL_CTL_ADD, json_sockfd, &accept_event) < 0)
    {
        syslog(LOG_DAEMON | LOG_ERR, "Error adding JSON fd to epoll: %s", strerror(errno));
        return 1;
    }
    accept_event.data.fd = serv_sockfd;
    accept_event.events = EPOLLIN;
    if (epoll_ctl(epollfd, EPOLL_CTL_ADD, serv_sockfd, &accept_event) < 0)
    {
        syslog(LOG_DAEMON | LOG_ERR, "Error adding INET fd to epoll: %s", strerror(errno));
        return 1;
    }

    struct epoll_event events[32];
    size_t const events_size = sizeof(events) / sizeof(events[0]);
    struct event remotes[64];
    size_t const remotes_size = sizeof(remotes) / sizeof(remotes[0]);
    size_t remotes_used = 0;
    struct event * remote_curr;
    while (1)
    {
        int nready = epoll_wait(epollfd, events, events_size, -1);

        for (int i = 0; i < nready; i++)
        {
            if (events[i].events & EPOLLERR)
            {
                syslog(LOG_DAEMON | LOG_ERR, "Epoll event error: %s", strerror(errno));
                return 1;
            }

            if (events[i].data.fd == json_sockfd ||
                events[i].data.fd == serv_sockfd)
            {
                if (remotes_used == remotes_size) {
                    syslog(LOG_DAEMON | LOG_ERR, "Max number of connections reached: %zu", remotes_used);
                    continue;
                }
                remote_curr = &remotes[remotes_used++];

                enum ev_type type = (events[i].data.fd == json_sockfd ? JSON_SOCK : SERV_SOCK);
                int sockfd = (events[i].data.fd == json_sockfd ? json_sockfd : serv_sockfd);
                socklen_t peer_addr_len;

                int newsockfd = accept(sockfd, (type == JSON_SOCK ? (struct sockaddr *)&remote_curr->event_json.peer
                                                                  : (struct sockaddr *)&remote_curr->event_serv.peer),
                                       &peer_addr_len);
                syslog(LOG_DAEMON, "New connection");
            }
        }
    }

    close(json_sockfd);
    close(serv_sockfd);
    unlink(json_sockpath);

    return 0;
}
