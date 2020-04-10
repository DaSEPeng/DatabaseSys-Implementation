/*
 * 打印leveldb的log文件到可读类型，假设文件不跨block
 *  
 * @author: 李鹏
 * @date: 04/09/2020
 *
 * 运行：g++ -std=c++0x print_log.cc -o print_log
 */

#include <string>     // std::string
#include <iostream>   // std::cout
#include <fstream>    // std::ifstream
#include <cstdlib>    // std::exit
#include <cassert>    // assert 
#include <cstdint>    // uint32_t  uint64_t

enum RecordType{
  kZeroType = 0x0,
  kFullType = 0x1,
  kFirstType = 0x2,
  kMiddleType = 0x3,
  kLastType = 0x4
};

enum ValueType{
  typedelete = 0x0,
  typevalue = 0x1
};

// 获取length长度 Varint变长编码
// Ref: https://www.cnblogs.com/jacksu-tencent/p/3389843.html
// https://blog.csdn.net/huntinux/article/details/51690665
void getVarint32Ptr(std::ifstream *fp, uint32_t *length){
  uint32_t result = 0;
  int shift = 0; // 移位大小
  while(shift<35){ // 最多5个字节
    uint8_t tmp = 0;  
    fp->read((char*)&tmp,sizeof(uint8_t));//读取一个字节
    if(tmp&128){                  // 如果后面还有字节，2的7次方=128
      result |= ((tmp&127)<<shift); // 取低7位，不断加在前面
    }else{
      result |= (tmp<<shift);
      *length = result;
      return;
    }
  shift += 7;//0 7 14 21 28
  }
 return;
}

int main(){
  // 打开日志文件
  std::string log_path = "/courseforleveldb-master/leveldb/testdb/000003.log";  // 这里相对的是当前目录 十六进制查看文件：hexdump -C 000003.log
  std::ifstream infile(log_path.c_str());
  assert(infile.is_open());
 
  while(infile.peek()!=EOF){
    // 读取校验码，占4个字节
    // 本身就是低位在前，高位在后读取的
    uint32_t check_num = 0;
    infile.read((char*)&check_num,sizeof(uint32_t));
    std::cout << "CheckNum: " << check_num << "\t";

    // 读取record length, 2个字节
    uint16_t record_length = 0;
    infile.read((char*)&record_length,sizeof(uint16_t));
    std::cout << "Record Length: " << record_length << "\t";

    // 读取Record Type，1个字节
    uint8_t record_type = 0;
    infile.read((char*)&record_type,sizeof(uint8_t));
    std::cout << "Record Type: ";
    switch (static_cast<RecordType>(record_type&0xff)){
      case kZeroType:
        std::cout<< "kZeroType\t";
        std::exit(0);
      case kFullType:
        std::cout<<"kFullType\t";
        break;
      case kFirstType:
        std::cout<<"kFirstType\t";
        break;
      case kMiddleType:
        std::cout<<"kMiddleType\t";
        break;
      case kLastType:
        std::cout<<"kLastType\t";
        break;
    }
    
    // 读取seq_num，跳表中的序号，8个字节
    uint64_t seq_num = 0;
    infile.read((char*)&seq_num,sizeof(uint64_t));

    // 读取entry num，按照batch写的时候，batch中有多少个，4个字节
    uint32_t entry_num = 0;
    infile.read((char*)&entry_num,sizeof(uint32_t));

    // 读取value type，标识是插入还是删除，1个字节
    uint8_t value_type = 0;
    infile.read((char*)&value_type,sizeof(uint8_t));
    
    // 读取key length，变长编码
    uint32_t key_length=0;
    getVarint32Ptr(&infile,&key_length);
    std::cout<<"keyLength: "<<key_length<<"\t";
    
    // 读取key
    char* key = new char[key_length];
    infile.read(key,key_length);
    std::cout<<"key: "<<key<<"\t";
    
    // 根据value type的类型，选择后面数据的读取方式
    switch(static_cast<ValueType>(value_type&0xff)){
      case typedelete:
        std::cout<<"del "<<std::endl;
        break;
      case typevalue:
        // 读取value length
        uint32_t value_length = 0;
        getVarint32Ptr(&infile,&value_length);
        std::cout<<"valueLength: " << value_length << "\t";
      
        // 读取value值
        char* value = new char[value_length];
        infile.read(value,value_length);
        std::cout<<"value: " << value << std::endl;
        break;
    }
      
  } // while的结尾
  std::cout<<"Log Printed!"<<std::endl;
  infile.close();
  return 0;
}
