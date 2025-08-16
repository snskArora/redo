#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>

int main(int argc, char **argv) {
    int port = 12121;
    int socket_fd, rec_len;
    struct sockaddr_in my_addr, remote_addr;
    char buffer[1024];
    socklen_t addr_size;

    socket_fd = socket(AF_INET, SOCK_DGRAM, 0);

    memset(&my_addr, '\0', sizeof(my_addr));
    my_addr.sin_family = AF_INET;
    my_addr.sin_port = htons(port);
    my_addr.sin_addr.s_addr = inet_addr("127.0.0.1");

    bind(socket_fd, (struct sockaddr*)&my_addr, sizeof(my_addr));  // Disregarding the local addr of file descriptor 
    addr_size = sizeof(remote_addr);
    char exit_key[] = "q\n";  // Handle a New Line character for exit key - shoutout netcat utility in interactive mode;

    while (1) {
        rec_len = recvfrom(socket_fd, buffer, 1024, 0, (struct sockaddr*)& remote_addr, &addr_size);
        buffer[rec_len] = '\0';
        if (0 == strcmp(buffer, exit_key)) {
            printf("exiting uselessness!!");
            break;
        } else {
            printf("Recieved,  %s ", buffer);
        }
    }
    return 0;
}
