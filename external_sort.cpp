#include <string>
#include <algorithm>
#include <unistd.h>
#include <fcntl.h>
#include <stdlib.h>
#include <assert.h>
#include <stdio.h>

class ExternalSorter {
public:
    bool Sort( const std::string& input_path, const std::string& output_path );

    // produce ceil( input_path_file_size / availableMemory ) runs
    // input file is an array of input_path_file_size / sizeof( T ) structures T
    //template< typename T, typename Compare >
    //void ProduceRuns( const std::string& input_path, Compare cmp );

private:
    const static size_t availableMemory = 1024 * 1024 * 1024; // 1 GB by default

};
/*
template< typename T, typename Compare >
void ExternalSorter::ProduceRuns( const std::string& input_path, Compare cmp ) {
    int fd = open( input_path.c_str(), O_RDONLY );
    if( fd < 0 ) {
        perror("Produce runs open file failed");
    }
    
    char* buff = new char[ availableMemory ];
    int readRes = 0;
    while( ( readRes = read( fd, buff, availableMemory ) ) ) {
        buff += readRes;
    }
    if( readRes < 0 ) {
        perror("Produce runs read file failed");
    }
}
*/
struct SimpleStruct {
    int userId = 0;
    int moneyCount = 0;

    SimpleStruct() : userId{ rand() }, moneyCount{ rand() } {}
};

template< typename T >
void GenerateTest( const std::string& output_path, int structsCount ) {
    int fd = open( output_path.c_str(), O_WRONLY | O_CREAT | O_EXCL );
    if( fd < 0 ) {
        perror("Generate test file creation failed");
    }

    printf( "Generating %d structs\n", structsCount );
    for( size_t i = 0; i < structsCount; ++i ) {
        SimpleStruct s;
        write( fd, &s, sizeof(s) );
    }

    close( fd );
}

int main( int argc, const char** argv ) {
    assert( argc == 2 );
    char path[256]{ 0 };
    sprintf( path, "./%s_test.data", argv[1]);
    GenerateTest<SimpleStruct>( path, 1024 * 1024 );
}