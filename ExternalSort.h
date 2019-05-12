#pragma once 

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

#define c_check( res, str ) \
    if( res < 0 ) { perror( str ); exit( EXIT_FAILURE ); }

#ifdef DEBUG 
    #define log(x) { std::cout << x << std::endl; } 
#else 
    #define log(x) {}
#endif

#define log_force(x) { std::cout << x << std::endl; } 

static std::string generateRunFileName( const std::string& inputPath, size_t runNumber, size_t epoch ) {
    return inputPath + "_run_" + std::to_string( epoch ) + "_" + std::to_string( runNumber );
}

const int newFilePerm = S_IWUSR | S_IRUSR | S_IRGRP | S_IROTH;
const size_t pageSize = getpagesize();

namespace ExternalSort {
    // external sort
    template< typename T, typename Compare >
    std::string Sort( const std::string& inputPath, Compare cmp, size_t availableMemory );

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
std::string ExternalSort::Sort( const std::string& inputPath, Compare cmp, size_t availableMemory ) {
    size_t runsCount = ExternalSort::ProduceRuns<T>( inputPath, cmp, availableMemory );
    log( "Sort: runs count " << runsCount );
    
    std::vector< std::string > oldEpoch;
    for( int i = 1; i <= runsCount; ++i ) {
        oldEpoch.push_back( generateRunFileName( inputPath, i, 1 ) );
    }
    std::vector< std::string > newEpoch;

    size_t minMemoryPerRun = 2 * pageSize;
    // at least 2 runs will be merged
    assert( 2 * minMemoryPerRun < availableMemory );

    size_t curMemory = 0;
    size_t resultNumber = 1;
    size_t epoch = 1;
    std::vector< std::string > batch;
    while( !oldEpoch.empty() ) {
        batch.clear();
        curMemory = 0;
        while( curMemory < availableMemory && !oldEpoch.empty() ) {
            batch.push_back( oldEpoch.back() );
            oldEpoch.pop_back();
            curMemory += minMemoryPerRun;
        }
        log( "Sort: curr iteration batch size " << batch.size() );
        std::string resultPath = generateRunFileName( inputPath, resultNumber, epoch + 1 );
        ++resultNumber;

        ExternalSort::MergeRuns<T>( batch, resultPath, cmp, availableMemory );
        newEpoch.push_back( resultPath );
        log( "Sort: curr iteration old epoch size " << oldEpoch.size() << ", new epoch size " << newEpoch.size() );

        for( const std::string& path : batch ) {
            c_check( unlink( path.c_str() ), ( std::string( "Sort: unlink failed " ) + path ).c_str() );
        }

        if( oldEpoch.empty() && newEpoch.size() != 1 ) {
            log( "Sort: copy new epoch to old epoch ");
            while( !newEpoch.empty( )) {
                oldEpoch.push_back( newEpoch.back() );
                newEpoch.pop_back();
            }
            resultNumber = 1;
            ++epoch;
            log( "Sort: old epoch size " << oldEpoch.size() << "; new epoch size " << newEpoch.size() );
        }
    }
    assert( newEpoch.size() == 1 );
    return newEpoch.front();

}

template< typename T, typename Compare >
size_t ExternalSort::ProduceRuns( const std::string& inputPath, Compare cmp, size_t availableMemory ) {
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

        log( "ProduceRuns: run " << runNumber << "; read " << totalRead << " of " << totalRead / sizeof( T ) << " from " << inputPath );
        
        T* structPtr = reinterpret_cast<T*>( buf.get() );
        std::sort( structPtr, structPtr + ( totalRead / sizeof( T ) ), cmp );

        std::string runPath = generateRunFileName( inputPath, runNumber, 1 );

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

        log( "ProduceRuns: run " << runNumber << "; total write " << totalWrite << " of " << totalWrite / sizeof( T ) << " to " << runPath );
        
        c_check( fsync( runFd ), "ProduceRuns: fsync failed" );
        c_check( close( runFd ), "ProduceRuns: close run file failed" );

        sortedSize += totalWrite;
    }

    c_check( close( fd ), "ProduceRuns: close source file failed " );

    return runNumber;
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
void ExternalSort::MergeRuns( const std::vector<std::string>& inputPaths, const std::string& outputPath, Compare cmp, size_t availableMemory ) {

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

        log( "MergeRuns: heap size " << readersCurrentValuesHeap.size() );
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
void ExternalSort::PrintRun( const std::string& inputPath, size_t runNumber, Print printFunc ) {
    std::string runFileName = generateRunFileName( inputPath, runNumber, 0 );
    int fd = open( runFileName.c_str(), O_RDONLY );
    c_check( fd, "PrintRun: open failed" );

    struct stat st;
    stat( inputPath.c_str(), &st );
    size_t size = st.st_size;
    log( "PrintRun: file size " << size );

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
    log( "PrintRun: total read " << totalRead << " bytes of " << structsCount << " from run " << runFileName );
    
    T* structPtr = reinterpret_cast<T*>( buf );
    for( size_t i = 0; i < structsCount; ++i ) {
        printFunc( *(structPtr + i) );
    }

    c_check( close( fd ), "PrintRun: close failed" );
}