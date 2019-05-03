#include <string>
#include <algorithm>
#include <unistd.h>
#include <fcntl.h>
#include <stdlib.h>
#include <assert.h>
#include <stdio.h>
#include <memory>
#include <sys/stat.h>

#define c_check( res, str ) \
    if( res < 0 ) { perror( str ); exit( EXIT_FAILURE ); }

static std::string generateRunFileName( const std::string& inputPath, size_t runNumber ) {
    return inputPath + "_run_" + std::to_string( runNumber );
}

static int newFilePerm = S_IWUSR | S_IRUSR | S_IRGRP | S_IROTH;

namespace ExternalSorting {
    const static size_t availableMemory = 1024 * 32; // 8 mb by default

    void Sort( const std::string& inputPath, const std::string& outputPath );

    // produce ceil( inputPath_file_size / availableMemory ) runs
    // input file is an array of inputPath_file_size / sizeof( T ) structures T
    template< typename T, typename Compare >
    void ProduceRuns( const std::string& inputPath, Compare cmp );

    // display run structure
    template< typename T, typename Print >
    void PrintRun( const std::string& inputPath, size_t runNumber, Print print );
};

template< typename T, typename Compare >
void ExternalSorting::ProduceRuns( const std::string& inputPath, Compare cmp ) {    
    int fd = open( inputPath.c_str(), O_RDONLY );
    c_check( fd, "Produce runs: open file failed" );
    
    struct stat st;
    stat( inputPath.c_str(), &st );
    size_t size = st.st_size;
    printf( "File size %zd\n", size );

    // check alignment
    assert( size % sizeof( T ) == 0 );

    size_t sortedSize = 0;
    int runNumber = 1;

    while( sortedSize < size ) {

        printf( "Current progress %zd / %zd\n", sortedSize, size );
        char* orig = new char[ availableMemory ];
        
        ssize_t readRes = 0;
        size_t remainRead = availableMemory;
        size_t totalRead = 0;
        while( readRes = read( fd, orig + totalRead, remainRead ) ) {
            remainRead -= readRes;
            totalRead += readRes;
        }
        c_check( readRes, "ProduceRuns: read file failed" );

        assert( totalRead > 0 );
        printf( "Total read %zd bytes of %zd structs from %s run %d\n", totalRead, totalRead / sizeof( T ), inputPath.c_str(), runNumber );
        
        T* structPtr = reinterpret_cast<T*>( orig );
        std::sort( structPtr, structPtr + ( totalRead / sizeof( T ) ), cmp );

        std::string runPath = generateRunFileName( inputPath, runNumber );

        int runFd = open( runPath.c_str(), O_WRONLY | O_CREAT | O_EXCL, newFilePerm );
        c_check( runFd, "ProduceRuns: create run file failed" );

        ssize_t writeRes = 0;
        size_t totalWrite = 0;
        size_t remainWrite = totalRead;
        while( ( writeRes = write( runFd, orig + totalWrite, remainWrite ) ) ) {
            remainWrite -= writeRes;
            totalWrite += writeRes;
            printf( "Writing %zd bytes in %d run\n", writeRes, runNumber );
            break;
        }
        c_check( writeRes, "ProduceRuns: write run failed");

        printf( "Total write %zd bytes of %zd structures from %s run %d\n", totalWrite, totalWrite / sizeof( T ), inputPath.c_str(), runNumber );
        c_check( close( runFd ), "ProduceRuns: close failed" );

        ++runNumber;

        sortedSize += totalWrite;
    }
}

template< typename T, typename Print >
void ExternalSorting::PrintRun( const std::string& inputPath, size_t runNumber, Print printFunc ) {
    std::string runFileName = generateRunFileName( inputPath, runNumber );
    int fd = open( runFileName.c_str(), O_RDONLY );
    c_check( fd, "PrintRun: open failed" );

    struct stat st;
    stat( inputPath.c_str(), &st );
    size_t size = st.st_size;
    printf( "PrintRun: File size %zd\n", size );

    char* orig = new char[ size ];    
    ssize_t readRes = 0;
    size_t remainRead = size;
    size_t totalRead = 0;
    while( readRes = read( fd, orig + totalRead, remainRead ) ) {
        remainRead -= readRes;
        totalRead += readRes;
    }
    c_check( readRes, "PrintRun: read file failed" );


    size_t structsCount = totalRead / sizeof( T );
    printf( "PrintRun: total read %zd bytes of %zd structs from %s run %zd\n", totalRead, structsCount, runFileName.c_str(), runNumber );
    
    T* structPtr = reinterpret_cast<T*>( orig );
    for( size_t i = 0; i < structsCount; ++i ) {
        printFunc( *(structPtr + i) );
    }

    close( fd );
}

struct SimpleStruct {
    int userId;
    int moneyCount;

    SimpleStruct() : userId{ rand() }, moneyCount{ rand() } {}
};

bool simpleStructCompare( const SimpleStruct& s1, const SimpleStruct& s2 ) {
    return s1.userId < s2.userId || (s1.userId == s2.userId && s1.moneyCount < s2.moneyCount);
}

template< typename T >
void GenerateTest( const std::string& outputPath, size_t structsCount ) {
    int fd = open( outputPath.c_str(), O_WRONLY | O_CREAT | O_EXCL, newFilePerm );
    c_check( fd, "GenerateTest: file creation failed");
    printf( "Generating %zd structs\n", structsCount );
    for( size_t i = 0; i < structsCount; ++i ) {
        SimpleStruct s;
        if( i % ( 1024 * 1024 ) == 0 ) {
            printf("%zd / %zd\n", i, structsCount );
        }
        c_check( write( fd, &s, sizeof(s) ), "GenerateTest: write fail" );
    }
    close( fd );
}

void PrintSimpleStruct( const SimpleStruct& s ) {
    printf( "UserId: %d MoneyCount: %d\n", s.userId, s.moneyCount );
}

int main( int argc, const char** argv ) {
    assert( argc == 2 );
    char path[256]{ 0 };
    sprintf( path, "./%s_test.data", argv[1]);
    GenerateTest<SimpleStruct>( path, 32 * 4 * 1024 );

    ExternalSorting::ProduceRuns<SimpleStruct>( path, simpleStructCompare );

    ExternalSorting::PrintRun<SimpleStruct>( path, 1, PrintSimpleStruct );
}