#define DEBUG
#include "ExternalSort.h"

const size_t memoryLimit = 256 * getpagesize();

struct SimpleStruct {
    int userId;
    int moneyCount;

    SimpleStruct() : userId{ rand() % 10000 }, moneyCount{ rand() % 40 } {}
};

bool simpleStructLessOrEq( const SimpleStruct& s1, const SimpleStruct& s2 ) {
    return s1.userId < s2.userId || ( s1.userId == s2.userId && s1.moneyCount <= s2.moneyCount );
}

template< typename T >
void GenerateTest( const std::string& outputPath, size_t structsCount ) {
    int fd = open( outputPath.c_str(), O_WRONLY | O_CREAT | O_EXCL, newFilePerm );
    c_check( fd, "GenerateTest: open failed" );

    size_t remainCount = structsCount;
    assert( memoryLimit % sizeof( SimpleStruct ) == 0 );
    size_t perIterationCount = memoryLimit / sizeof( SimpleStruct );
    while( remainCount > 0 ) {
        size_t currIterationCount = std::min( remainCount, perIterationCount );
        std::shared_ptr<SimpleStruct> currStructs( new SimpleStruct[currIterationCount] );
        log( "GenerateTest: remain " << remainCount << " structs" );
        
        ssize_t writeRes = 0;
        size_t remainWrite = sizeof(SimpleStruct) * currIterationCount;
        size_t totalWrite = 0;
        while( writeRes = write( fd, currStructs.get() + totalWrite, remainWrite ) ) {
            remainWrite -= writeRes;
            totalWrite += writeRes;
        }
        c_check( writeRes, "GenerateTest: write failed" );

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

    std::string resultPath = ExternalSort::Sort<SimpleStruct>( path, simpleStructLessOrEq, memoryLimit );
    
    for( size_t i = 0; i < 20; ++i ) {
        log( "" );
    }
    log( "Final result" );
    RunReader<SimpleStruct> reader( resultPath, memoryLimit );
    size_t readId = 1;
    bool increasing = true;
    SimpleStruct prev;
    while( reader.HasMore() ) {
        log( "Result id " << readId );
        
        SimpleStruct curr = reader.PopTop();
        PrintSimpleStruct( curr );

        if( readId > 1 ) {
            increasing = simpleStructLessOrEq( prev, curr );
        }
        
        log( "Increasing " << increasing );
        assert( increasing );

        prev = curr;
        ++readId;
    }
}