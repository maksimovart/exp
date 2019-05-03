#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>

int main( int argc, char** argv ) {
    int my_socket = socket( AF_INET, SOCK_STREAM, 0);
    if( my_socket < 0 ) {
        perror("Socket creation failure");
        exit(EXIT_FAILURE);
    }

    sockaddr_in addr;
    bzero( &addr, sizeof(addr) );
    addr.sin_family = AF_INET;
    addr.sin_port = htons(7000);
    addr.sin_addr.s_addr = inet_addr("127.0.0.1");

    if( connect( my_socket, reinterpret_cast<sockaddr*>(&addr), sizeof(addr) ) < 0 ) {
        perror("Connect failure");
        exit(EXIT_FAILURE);
    }
    
    char buf[256] = "Test1";
    write( my_socket, buf, 256 );
    exit(EXIT_SUCCESS);
}