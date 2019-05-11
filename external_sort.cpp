#include <string>
#include <algorithm>
#include <unistd.h>
#include <fcntl.h>
#include <stdlib.h>
#include <assert.h>
#include <stdio.h>
#include <memory>
#include <sys/stat.h>
#include <cstring>
#include <iostream>
#include <utility>

#define DEBUG

#define c_check( res, str ) \
    if( res < 0 ) { perror( str ); exit( EXIT_FAILURE ); }

#ifdef DEBUG 
    #define log(x) { std::cout << x << std::endl; } 
#else 
    #define log(x) {}
#endif

static std::string generateRunFileName( const std::string& inputPath, size_t runNumber ) {
    return inputPath + "_run_" + std::to_string( runNumber );
}

const int newFilePerm = S_IWUSR | S_IRUSR | S_IRGRP | S_IROTH;
const size_t pageSize = getpagesize();

namespace ExternalSorting {
    const static size_t memoryLimit = 16 * pageSize; // 16 pages 

    // produce ceil( inputPath_file_size / availableMemory ) runs
    // input file is an array of inputPath_file_size / sizeof( T ) structures T
    template< typename T, typename Compare >
    size_t ProduceRuns( const std::string& inputPath, Compare cmp, size_t availableMemory );

    // merge runs into single file
    template< typename T, typename Compare >
    void MergeRuns( const std::vector<std::string>& inputPaths, const std::string& outputPath, Compare cmp, size_t availableMemory );

    // display run structure
    template< typename T, typename Print >
    void PrintRun( const std::string& inputPath, size_t runNumber, Print print );

};

template< typename T, typename Compare >
size_t ExternalSorting::ProduceRuns( const std::string& inputPath, Compare cmp, size_t availableMemory ) {
    assert( sizeof( T ) < availableMemory );

    int fd = open( inputPath.c_str(), O_RDONLY );
    c_check( fd, "ProduceRuns: open source file failed" );
    posix_fadvise( fd, 0, 0, POSIX_FADV_SEQUENTIAL );

    struct stat st;
    stat( inputPath.c_str(), &st );
    size_t size = st.st_size;
    
    assert( size % sizeof( T ) == 0 );

    size_t sortedSize = 0;
    size_t runNumber = 0;

    std::shared_ptr<char> buf( new char[ availableMemory ] );
    while( sortedSize < size ) {
        ++runNumber;
        memset( buf.get(), 0, availableMemory );
        log( "ProduceRuns: current progress " << sortedSize << "/" << size );
        
        ssize_t readRes = 0;
        size_t remainRead = availableMemory;
        size_t totalRead = 0;
        while( readRes = read( fd, buf.get() + totalRead, remainRead ) ) {
            remainRead -= readRes;
            totalRead += readRes;
        }
        c_check( readRes, "ProduceRuns: read file failed" );

        printf( "ProduceRuns: run %zd; read %zd bytes of %zd structures from %s\n", runNumber, totalRead, totalRead / sizeof( T ), inputPath.c_str() );
        
        T* structPtr = reinterpret_cast<T*>( buf.get() );
        std::sort( structPtr, structPtr + ( totalRead / sizeof( T ) ), cmp );

        std::string runPath = generateRunFileName( inputPath, runNumber );

        int runFd = open( runPath.c_str(), O_WRONLY | O_CREAT | O_EXCL, newFilePerm );
        c_check( runFd, "ProduceRuns: open run file failed" );

        ssize_t writeRes = 0;
        size_t remainWrite = totalRead;
        size_t totalWrite = 0;
        while( ( writeRes = write( runFd, buf.get() + totalWrite, remainWrite ) ) ) {
            remainWrite -= writeRes;
            totalWrite += writeRes;
        }
        c_check( writeRes, "ProduceRuns: write run failed");

        printf( "ProduceRuns: run %zd; total write %zd bytes of %zd structures to %s \n", runNumber, totalWrite, totalWrite / sizeof( T ), runPath.c_str() );
        
        c_check( fsync( runFd ), "ProduceRuns: fsync failed" );
        c_check( close( runFd ), "ProduceRuns: close run file failed" );

        sortedSize += totalWrite;
    }

    c_check( close( fd ), "ProduceRuns: close source file failed " );
}

template< typename T >
class RunReader {
public:
    RunReader( const std::string& _path, size_t _availableMemory ) : 
        path( _path ),
        availableMemory( _availableMemory ),
        buf( new char[ availableMemory ] )
    {
        assert( availableMemory > sizeof( T ) );

        fd = open( path.c_str(), O_RDONLY );
        c_check( fd, ( std::string("RunReader: open failed") + path ) .c_str() );
        posix_fadvise( fd, 0, 0, POSIX_FADV_SEQUENTIAL );

        assert( buf.get() != nullptr );

        stat( path.c_str(), &st );
        assert( st.st_size % sizeof( T ) == 0 );

        structsPerRead = availableMemory / sizeof( T );

        log( "RunReader: " << path << " initialization; availableMemory " << availableMemory << ", structsPerRead " << structsPerRead << ", file size " << st.st_size );
    }

    RunReader( const RunReader& ) = delete;
    RunReader& operator=( const RunReader& ) = delete;
    RunReader( RunReader&& rhs ) = delete;
    RunReader& operator=( RunReader&& rhs ) = delete;

    ~RunReader() {
        c_check( fsync(fd), ( std::string("RunReader: fsync failed") + path ) .c_str() );
        c_check( close(fd), ( std::string("RunReader: close failed") + path ) .c_str() );
    }

    T GetTop() {
        assert( GetPoppedStructsCount() < GetTotalStructsCount() );
        if( totalReadSize == 0 ) {
            readNextPortion();
        }
        return reinterpret_cast<T*>( buf.get() )[ GetPoppedStructsCount() % structsPerRead  ];
    }

    T PopTop() {
        assert( GetPoppedStructsCount() < GetTotalStructsCount() );
        T value = GetTop();
        ++popedStructsCount;
        if( GetPoppedStructsCount() % structsPerRead == 0 
            && GetPoppedStructsCount() < GetTotalStructsCount() ) {
            readNextPortion();
        }
        return value;
    }

    size_t GetTotalStructsCount() const {
        return st.st_size / sizeof( T );
    }

    size_t GetPoppedStructsCount() const {
        return popedStructsCount;
    }

    bool HasMore() const {
        return GetPoppedStructsCount() < GetTotalStructsCount();
    }

private:
    int fd;
    std::string path;

    size_t availableMemory = 0;
    std::shared_ptr<char> buf;

    struct stat st;

    size_t totalReadSize = 0;
    size_t structsPerRead = 0;
    size_t popedStructsCount = 0;

    // read next availableMemory / sizeof( T ) structs
    // current buffer will be overwritten
    bool readNextPortion() {
        char* ptr = buf.get();

        ssize_t readRes = 0;
        size_t remainRead = structsPerRead * sizeof( T );
        size_t totalRead = 0;
        log( "RunReader: " << path << " need read " << remainRead );
        while( readRes = read( fd, ptr + totalRead, remainRead ) ) {
            totalRead += readRes;
            remainRead -= readRes;
            totalReadSize += readRes;

            log( "RunReader: " << path << " read " << readRes );
            log( "RunReader: " << path << " needRead " << remainRead );
        }
        c_check( readRes, ( std::string( "RunRead: " ) + path + " read failed" ).c_str() );
        log( "RunReader: " << path << " total read size " << totalReadSize << " bytes (of " << st.st_size << " file bytes)" );

    }
};

template< typename T, typename Compare >
void ExternalSorting::MergeRuns( const std::vector<std::string>& inputPaths, const std::string& outputPath, Compare cmp, size_t availableMemory ) {

    using TReader = RunReader<T>;
    using TReaderPtr = std::shared_ptr<TReader>;
    using TReaderValue = std::pair< TReaderPtr, T >;

    size_t pagesCount = availableMemory / pageSize;
    assert( pagesCount > 1 );

    size_t pagesPerRun = ( pagesCount - 1 ) / inputPaths.size(); // -1 for write buffer
    // at least one page per run
    assert( pagesPerRun > 0 );

    size_t memoryPerRun = pageSize * pagesPerRun;
    size_t structsPerRun = memoryPerRun / sizeof( T );
    // at least one struct can be processed
    assert( structsPerRun > 0 );

    std::vector< TReaderPtr > readers;
    
    auto readerValueCompare = [ cmp ]( const TReaderValue& left, const TReaderValue& right ) -> bool {
        // std::heap is max heap, we need min heap => inverse result
        return !cmp( left.second, right.second );
    };

    // for debug purpose
    size_t totalStructsCount = 0;

    std::vector< TReaderValue > readersCurrentValuesHeap;
    for( const std::string& path : inputPaths ) {
        TReaderPtr reader( new TReader( path, memoryPerRun ) );
        if( !reader->HasMore() ) {
            continue;
        }
        totalStructsCount += reader->GetTotalStructsCount();
        readers.push_back( reader );

        T value = reader->PopTop();
        readersCurrentValuesHeap.push_back( std::make_pair( reader, value ) );
        std::push_heap( readersCurrentValuesHeap.begin(), readersCurrentValuesHeap.end(), readerValueCompare );
    }

    // temporary 256 pages for write
    // todo: rewrite for using pages for availableMemory
    size_t writeBufSize = pageSize * 256;
    size_t writeBufMaxStructsCount = writeBufSize / sizeof( T );
    size_t writeBufCurrentStructs = 0;
    std::shared_ptr<char> writebuf( new char[ pageSize * 256 ] );
    
    int writeFd = open( outputPath.c_str(), O_WRONLY | O_CREAT | O_EXCL, newFilePerm );
    c_check( writeFd, ( std::string("MergeRuns: result file creation failed ") + outputPath ).c_str() );
    posix_fadvise( writeFd, 0, 0, POSIX_FADV_SEQUENTIAL );

    size_t processedStructsCount = 0;
    while( !readersCurrentValuesHeap.empty() ) {
        if( writeBufCurrentStructs == writeBufMaxStructsCount ) {
            ssize_t writeRes = 0;
            size_t remainWrite = writeBufCurrentStructs * sizeof( T );
            size_t totalWrite = 0;
            while( writeRes = write( writeFd, writebuf.get() + totalWrite, remainWrite ) ) {
                remainWrite -= writeRes;
                totalWrite += writeRes;
            }
            c_check( writeRes, "MergeRuns: result file write failed" );
            writeBufCurrentStructs = 0;
        }

        std::pop_heap( readersCurrentValuesHeap.begin(), readersCurrentValuesHeap.end(), readerValueCompare );
        TReaderValue rv = readersCurrentValuesHeap.back();
        readersCurrentValuesHeap.pop_back();
        
        T& value = rv.second;
        T* writePlace = reinterpret_cast<T*>( writebuf.get() );
        writePlace += writeBufCurrentStructs;
        *writePlace = value;
        ++writeBufCurrentStructs;
        ++processedStructsCount;

        log( "MergeRuns: heap size " << readersCurrentValuesHeap.size() << ", max value " );
        PrintSimpleStruct( value );
        log( "MergeRuns: processed " << processedStructsCount << "/" << totalStructsCount );

        TReaderPtr reader( rv.first );
        if( !reader->HasMore() ) {
            log( "MergeRuns: reader has no more" );
            readers.erase( std::find( readers.begin(), readers.end(), reader ) );
            continue;
        }

        readersCurrentValuesHeap.push_back( std::make_pair( reader, reader->PopTop() ) );
        std::push_heap( readersCurrentValuesHeap.begin(), readersCurrentValuesHeap.end(), readerValueCompare );
    }

    ssize_t writeRes = 0;
    size_t remainWrite = writeBufCurrentStructs * sizeof( T );
    size_t totalWrite = 0;
    while( writeRes = write( writeFd, writebuf.get() + totalWrite, remainWrite ) ) {
        remainWrite -= writeRes;
        totalWrite += writeRes;
    }
    c_check( writeRes, "MergeRuns: result file write failed" );
    c_check( fsync( writeFd ), "MergeRuns: result file fsync failed" );
    c_check( close( writeFd ), "MergeRuns: result file close failed" );
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

    char* buf = new char[ size ];    
    ssize_t readRes = 0;
    size_t remainRead = size;
    size_t totalRead = 0;
    while( readRes = read( fd, buf + totalRead, remainRead ) ) {
        remainRead -= readRes;
        totalRead += readRes;
    }
    c_check( readRes, "PrintRun: read file failed" );


    size_t structsCount = totalRead / sizeof( T );
    printf( "PrintRun: total read %zd bytes of %zd structs from %s run %zd\n", totalRead, structsCount, runFileName.c_str(), runNumber );
    
    T* structPtr = reinterpret_cast<T*>( buf );
    for( size_t i = 0; i < structsCount; ++i ) {
        printFunc( *(structPtr + i) );
    }

    c_check( close( fd ), "PrintRun: close failed" );
}

struct SimpleStruct {
    int userId;
    int moneyCount;

    SimpleStruct() : userId{ rand() % 10000 }, moneyCount{ rand() % 40 } {}
};

bool simpleStructCompare( const SimpleStruct& s1, const SimpleStruct& s2 ) {
    return s1.userId < s2.userId || (s1.userId == s2.userId && s1.moneyCount < s2.moneyCount);
}

template< typename T >
void GenerateTest( const std::string& outputPath, size_t structsCount ) {
    int fd = open( outputPath.c_str(), O_WRONLY | O_CREAT | O_EXCL, newFilePerm );
    c_check( fd, "GenerateTest: open failed");

    size_t remainCount = structsCount;
    assert( ExternalSorting::memoryLimit % sizeof( SimpleStruct ) == 0 );
    size_t perIterationCount = ExternalSorting::memoryLimit / sizeof( SimpleStruct );
    while( remainCount > 0 ) {
        size_t currIterationCount = std::min( remainCount, perIterationCount );
        std::shared_ptr<SimpleStruct> currStructs( new SimpleStruct[currIterationCount] );
        printf( "GenerateTest: remain %zd structs\n", remainCount );
        
        ssize_t writeRes = 0;
        size_t remainWrite = sizeof(SimpleStruct) * currIterationCount;
        size_t totalWrite = 0;
        while( writeRes = write( fd, currStructs.get() + totalWrite, remainWrite ) ) {
            remainWrite -= writeRes;
            totalWrite += writeRes;
        }
        c_check( writeRes, "GenerateTest: write failed");

        remainCount -= currIterationCount;
    }
    c_check( close( fd ), "GenerateTest: close failed" );
}

void PrintSimpleStruct( const SimpleStruct& s ) {
    log( "(" << s.userId << ", " << s.moneyCount << ")" );
}

int main( int argc, const char** argv ) {
    assert( argc == 3 );
    char path[256]{ 0 };
    sprintf( path, "./%s_test.data", argv[1]);
    size_t structuresCount = std::stoul( argv[2] ); 
    GenerateTest<SimpleStruct>( path, structuresCount );

    size_t runsCount = ExternalSorting::ProduceRuns<SimpleStruct>( path, simpleStructCompare, ExternalSorting::memoryLimit );

    std::string runPath = generateRunFileName( path, 1 );
    {
        RunReader<SimpleStruct> reader( runPath, 1024 );
        size_t readId = 1;;
        while( reader.HasMore() ) {
            std::cout << readId++ << std::endl;
            PrintSimpleStruct( reader.PopTop() );
        }
    }

    std::vector< std::string > runPaths;
    for( int i = 1; i <= 10; ++i ) {
        runPaths.push_back( generateRunFileName( path, i ) );
    }
    std::string resultPath = std::string( path ) + "__result";
    ExternalSorting::MergeRuns< SimpleStruct >( runPaths, resultPath, simpleStructCompare, ExternalSorting::memoryLimit );

    {
        for( size_t i = 0; i < 20; ++i ) {
            std::cout << std::endl;
        }
        std::cout << "Final result" << std::endl;
        RunReader<SimpleStruct> reader( resultPath, 1024 );
        size_t readId = 1;;
        while( reader.HasMore() ) {
            std::cout << readId++ << std::endl;
            PrintSimpleStruct( reader.PopTop() );
        }
    }
    //ExternalSorting::PrintRun<SimpleStruct>( path, 1, PrintSimpleStruct );
}