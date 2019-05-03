#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <iostream>

int process_client( int client_handle, sockaddr_in& addr, socklen_t& length ) {
    printf("Processing client...\n");
    char buf[256];
    read( client_handle, buf, 256 );
    printf("%s\n", buf);
    return 0;
}

int main( int argc, char** argv ) {

    char* pt = (char*) new char;
    int pages = 0;
    for( int i = 0; i < 4 * 1024 * 1024; ++i ) {
        std::cout << pages << ", " << (int) pt << ", " << ((int) pt) % 4096 << "\n";
        std::cout << *pt << "\n";
        ++pt;
        if( i % 4096 == 0 ) 
            ++pages;
    }

    int server_socket = socket( AF_INET, SOCK_STREAM, 0);
    if( server_socket < 0 ) {
        perror("Socket creation failure");
        exit(EXIT_FAILURE);
    }

    sockaddr_in addr;
    
    bzero( &addr, sizeof(addr) );
    addr.sin_family = AF_INET;
    addr.sin_port = htons(7000);
    addr.sin_addr.s_addr = INADDR_ANY;

    if( bind( server_socket, reinterpret_cast<sockaddr*>(&addr), sizeof(addr) ) < 0 ) {
        perror("Bind failure");
        exit(EXIT_FAILURE);
    }

    if( listen( server_socket, 1024 ) < 0 ) {
        perror("Listen failure");
        exit(EXIT_FAILURE);
    }
    
    int client_handle;
    sockaddr_in client_addr;
    socklen_t client_length = sizeof( addr );
    printf("Start to listening\n");
    while( client_handle = accept( server_socket, reinterpret_cast<sockaddr*>(&client_addr), &client_length ) ) {
        pid_t pid = fork();
        if( pid < 0 ) {
            perror("Fork failure");
            exit(EXIT_FAILURE);
        }
        if( pid != 0 ) {
            printf("Child forked... Continue listening\n");
            continue;
        }
        close( server_socket );
        process_client( client_handle, client_addr, client_length );
        exit(EXIT_SUCCESS);
    }
}