#include <arpa/inet.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include "config.h"
#ifdef USE_JSON
#include "jsmn/jsmn.h"
#endif

static char serv_listen_addr[INET_ADDRSTRLEN] = DISTRIBUTOR_HOST;
static uint16_t serv_listen_port = DISTRIBUTOR_PORT;

int main(void)
{
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in remote_addr = {};
    socklen_t remote_addrlen = sizeof(remote_addr);
    uint8_t buf[NETWORK_BUFFER_MAX_SIZE];
    size_t buf_used = 0;
    size_t json_start = 0;
    unsigned long long int buf_wanted = 0;
#ifdef USE_JSON
    jsmn_parser parser;
    jsmntok_t tokens[128];
#endif

    if (sockfd < 0)
    {
        perror("socket");
        return 1;
    }

    remote_addr.sin_family = AF_INET;
    if (inet_pton(AF_INET, &serv_listen_addr[0], &remote_addr.sin_addr) != 1)
    {
        perror("inet_pton");
        return 1;
    }
    remote_addr.sin_port = htons(serv_listen_port);

    if (connect(sockfd, (struct sockaddr *)&remote_addr, remote_addrlen) != 0)
    {
        perror("connect");
        return 1;
    }

    while (1)
    {
        errno = 0;
        ssize_t bytes_read = read(sockfd, buf + buf_used, sizeof(buf) - buf_used);

        if (bytes_read <= 0 || errno != 0)
        {
            break;
        }

        buf_used += bytes_read;
        if (buf_wanted == 0)
        {
            char * json_str_start = NULL;
            errno = 0;
            /* the first bytes are the textual representation of the following JSON string */
            buf_wanted = strtoull((char *)buf, &json_str_start, 10);
            json_start = (uint8_t *)json_str_start - buf;
            buf_wanted += json_start;

            if (errno == ERANGE)
            {
                buf_used = 0;
                buf_wanted = 0;
                fprintf(stderr, "Size of JSON exceeds limit\n");
                continue;
            }
            if ((uint8_t *)json_str_start == buf)
            {
                fprintf(stderr, "Missing size before JSON string: %.*s\n", (int)buf_used, buf);
                buf_used = 0;
                buf_wanted = 0;
                continue;
            }
            if (buf_wanted > sizeof(buf))
            {
                fprintf(stderr, "BUG: JSON string too big: %llu > %zu\n", buf_wanted, sizeof(buf));
                buf_used = 0;
                buf_wanted = 0;
                continue;
            }
        }

        /* buffered enough data (full JSON String) ? */
        if (buf_wanted > buf_used)
        {
            continue;
        }
        /* after buffering complete, last character should always be a '}' (end of object) */
        if (buf[buf_wanted - 1] != '}')
        {
            fprintf(stderr, "Invalid JSON string: %.*s\n", (int)buf_wanted, buf);
            buf_used = 0;
            buf_wanted = 0;
            continue;
        }

#ifdef USE_JSON
        int r;
        jsmn_init(&parser);
        r = jsmn_parse(&parser, (char *)(buf + json_start), buf_wanted - json_start,
                       tokens, sizeof(tokens) / sizeof(tokens[0]));
        if (r < 0 || tokens[0].type != JSMN_OBJECT) {
            fprintf(stderr, "JSON parsing failed with return value %d at position %u\n", r, parser.pos);
            fprintf(stderr, "JSON string: '%.*s'\n", (int)(buf_wanted - json_start), (char *)(buf + json_start));
        }

        for (int i = 1; i < r; i++) {
            if (i % 2 ==  1) {
                printf("[%.*s : ", tokens[i].end - tokens[i].start,
                       (char *)(buf + json_start) + tokens[i].start);
            } else {
                printf("%.*s] ", tokens[i].end - tokens[i].start,
                       (char *)(buf + json_start) + tokens[i].start);
            }
        }
        printf("EoF\n");
#else
        printf("RECV[%llu,%zd]: '%.*s'\n\n", buf_wanted, bytes_read, (int)buf_wanted, buf);
#endif
        memmove(buf, buf + buf_wanted, buf_used - buf_wanted);
        buf_used -= buf_wanted;
        buf_wanted = 0;
    }

    return 0;
}