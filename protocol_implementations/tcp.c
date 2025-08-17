#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>

int main() {
    int PORT = 12180;
	int socket_fd;
	struct sockaddr_in server_addr;

	int new_socket;
	struct sockaddr_in new_addr;

	socklen_t addr_size;
	char buffer[1024];

	socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_fd == -1) {
        printf("Error while creating the socket file descriptor");
        return 7;
    }
	printf("Server Socket Created Sucessfully.\n");
	memset(&server_addr, '\0', sizeof(server_addr));

	server_addr.sin_family = AF_INET;
	server_addr.sin_port = htons(PORT);
	server_addr.sin_addr.s_addr = inet_addr("127.0.0.1");

	int err = bind(socket_fd, (struct sockaddr*)&server_addr, sizeof(server_addr));
    if (err != 0) {
        printf("Error while assigning the socket a name, err_code returned: %d.", err);
        return err;
    }
	printf("-> Bind to Port number %d.\n", PORT);

	listen(socket_fd, 3); // for local testing 3 seems enough queue length and ignoring err check
	printf("-> Listening...\n");
    // Skipping the recieve, looping and multi-threading, impatient to start with https

	new_socket = accept(socket_fd, (struct sockaddr*)&new_addr, &addr_size);

	strcpy(buffer, "Hello");
	send(new_socket, buffer, strlen(buffer), 0);
	printf("-> Closing the connection.\n");

    return 0;

}
