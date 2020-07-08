/*
 * 数据库系统实现实验7：在SSTABLE中查找key
 * 
 * @author: 李鹏
 * @date: 2020/07/08
 *
 * 运行程序：
 *   g++ -std=c++0x search_sst.cc -o search_sst
 *   ./search_sst
 */

 
#include <cstdio>
#include <iostream>
#include <string>
#include <vector>
#include <cstring>

using namespace std;

// BlockIndex的结构体
struct BlockIndex
{
    uint32_t key_length, value_length;
    char* limit_key;
    uint64_t offset, size;
    BlockIndex(int len){
        limit_key = new char[len];
        memset(limit_key,'\0',sizeof(limit_key));
    }

};

// 得到文件file_path的大小
int getFileSize(const string &file_path) {
    int size = 0;
    FILE* fp = nullptr;

    fp = fopen(file_path.c_str(), "r");
    if (nullptr == fp) {
        return size;
    }

    fseek(fp, 0L, SEEK_END); // 将fp指向文件尾
    size = ftell(fp); // 得到fp的相对于文件首的偏移字节数
    cout << "File Size:" << size << endl;

    fclose(fp);
    return size;
}

// 处理32位变长编码
char* GetVarint32(char* p,int size,uint32_t* value, uint32_t &bytes){
    char* limit = p + size;
    uint32_t result = 0;
    bytes = 0;
    for (uint32_t shift = 0;shift < 32 && p < limit;shift += 7){
        uint32_t byte = *(static_cast<char*>(p));
        p++;
        bytes++; // 实际占用了几个字节
        if(byte & 128){
            result|= ((byte & 127) << shift);
        } else {
            result |= (byte << shift);
            *value = result;
            return p;
        }
    }
    return nullptr;
}

// 处理64位变长编码
char* GetVarint64(char* p, int size, uint64_t* value) {
    char* limit = p + size;
    uint64_t result = 0;
    for (uint32_t shift = 0; shift < 64 && p < limit; shift += 7) {
        uint64_t byte = *(static_cast<char*>(p));
        p++;
        if (byte & 128) {
            result |= ((byte & 127) << shift);
        } else {
            result |= (byte << shift);
            *value = result;
            return p;
        }
    }
    return nullptr;
}

// 主函数
int main() {
    // 文件地址
    string file_path = "/courseforleveldb-master/leveldb/lab2_db/000005.ldb";

    // 获取整个文件大小
    int file_size = getFileSize(file_path);

    /********************************** 读取footer  **********************************/
    // 将文件指针fp指向footer开头，并将footer部分数据读入内存
    FILE* fp = fopen(file_path.c_str(), "r");
    fseek(fp, -48L, SEEK_END);
    char* footer = (char *)malloc(48 * sizeof(char));
    fgets(footer, 48, fp);

    // 依次得到metablock和indexblock的信息，并打印
    uint64_t metablock_offset, metablock_size, indexblock_offset, indexblock_size;
    footer = GetVarint64(footer, 10, &metablock_offset);
    footer = GetVarint64(footer, 10, &metablock_size); // 这里也不包含压缩和CRC，因此也少5个字节
    footer = GetVarint64(footer, 10, &indexblock_offset);
    footer = GetVarint64(footer, 10, &indexblock_size); // 注意这里的size不包含压缩和CRC，因此少5个字节
    cout << "Metablock offset:" << metablock_offset << endl
         << "Metablock size:" << metablock_size << endl
         << "Indexblock offset:" << indexblock_offset << endl
         << "Indexblock size:" << indexblock_size << endl;
    cout << endl;
    
    /********************************* 读取index block *******************************/

    // 得到indexblock的具体信息
    char* indexblock = (char *)malloc((indexblock_size + 1) * sizeof(char));
    fseek(fp, indexblock_offset,SEEK_SET);
    fread(indexblock,sizeof(char),indexblock_size,fp); // 将indexblock中的信息读入指针indexblock的位置

    indexblock_size -= 1; // 数组以0开始
    uint32_t num_of_indexblock = 0,k=0;

    while(k < 4){ // 这里用的是restart_count，4个字节，里面对应着有多少个index_block_record,也是多少个datablock
        num_of_indexblock = num_of_indexblock << 8|(uint32_t)indexblock[indexblock_size]; //大端小端存储
        k++;
        indexblock_size--; // 字节数
        }
    cout << "NUM of indexblock_record: " << num_of_indexblock << endl;

    
    // 从每个index_block_record中读取datablock的偏移和大小
    int i = 0;
    vector<BlockIndex> bx;
    while (i < num_of_indexblock){
        // 注意，这里的key为一个大于等于当前datablock中最大的key且小于下一个block 中最小的key
        indexblock ++; // 共享长度，这里确定是0，所以共享长度只有1
        uint32_t key_length, value_length, bytes; // 这里key_length其实是no_shared_key_length
        indexblock = GetVarint32(indexblock,5,&key_length,bytes); // 非共享长度
	key_length -= 8; // 变长编码要+8字节
        BlockIndex now(key_length+1);

        indexblock = GetVarint32(indexblock,5,&value_length,bytes); // value length，其实不是真正的
       
        now.key_length = key_length;
        now.value_length = value_length;
        memcpy(now.limit_key,indexblock,key_length); // limit key

        indexblock += key_length; // 跳过limit key
        indexblock += 8; // 跳过value_type 和 seq_num

        // cout << "key length: " << key_length << endl;
        // cout << "limit key: " << now.limit_key << endl; //生成的key

        uint64_t block_offset, block_size;
        indexblock = GetVarint64(indexblock,10, &block_offset);
        indexblock = GetVarint64(indexblock,10, &block_size);
        now.offset = block_offset;
        now.size = block_size;

        // cout << "block_offset: " << block_offset << endl;
        // cout << "block_size: " << block_size << endl; // 注意这里也不包含压缩类型和CRC校验

        bx.push_back(now);
        i++;
    }
 

    /*************************** Search **********************************/
    // search
    
    while(true){
        cout << "Input the search key: ";
        int found_flag = 0;
        string skey_str;
        cin >> skey_str;
        if (skey_str == "-1") break;

        char * skey = (char*)skey_str.c_str();
 
        uint32_t l = 0,r = num_of_indexblock;

        // 二分查找
        while (r > l+1){
            uint32_t mid = (l + r)/2;
            if(strcmp(bx[mid].limit_key,skey)<0){
              l = mid +1;
            }
            else{
              r = mid;
            }
        }

        cout << "block id: " << l << endl;

        // 答案最可能在编号为l的data_block里
        uint64_t block_offset = bx[l].offset, block_size = bx[l].size;
        uint64_t limit_offset = block_offset + block_size -1; // 限制偏移
        char *start_number = new char();
        uint32_t num_of_restart = 0, k=0; // 重启点

        while(k < 4){
            fseek(fp,limit_offset,SEEK_SET);
            fread(start_number,sizeof(char),1,fp);
            num_of_restart = num_of_restart << 8|(uint32_t)*start_number;
            k++;
            limit_offset--;// 去掉restart count
        }
        limit_offset -= num_of_restart * 4; // 去掉restart_offset

        // fseek(fp,block_offset,SEEK_SET); // 注意这里是block的起始位置
        string lastkey = "";
        uint32_t record_i = 0;
        while(block_offset <= limit_offset){ // 处理每一条data_record
            cout << endl;
            cout << "    Read data_record " << record_i + 1 << ":" << endl;
            record_i++;

            // shared key length
            char* length = new char[5];
            uint32_t shared_length, no_shared_length,value_length,bytes;
            fseek(fp,block_offset,SEEK_SET); // 注意这里
            fread(length,sizeof(char),5,fp);
            GetVarint32(length,5,&shared_length,bytes);
            block_offset += bytes;

            // no-shared key length
            fseek(fp,block_offset,SEEK_SET);
            fread(length,sizeof(char),5,fp);
            GetVarint32(length,5,&no_shared_length,bytes);
            no_shared_length -= 8;
            block_offset += bytes;

            // value length
            fseek(fp,block_offset,SEEK_SET);
            fread(length,sizeof(char),5,fp);
            GetVarint32(length,5,&value_length,bytes);
            block_offset += bytes;

            cout << "    shared length: " << shared_length << endl 
                 << "    no shared length: " << no_shared_length << endl
                 << "    value length: " << value_length << endl;


            // key
            char* key = new char[no_shared_length+1];
            memset(key,'\0',sizeof(key));

            fseek(fp,block_offset,SEEK_SET);
            fread(key,sizeof(char),no_shared_length,fp);

            string now_key;
            if (shared_length!=0){
                now_key = lastkey.substr(0,shared_length) + string(key);
            }
            else{
                now_key = key;
            }

            cout << "    now key: " << now_key << endl;
            cout << "    skey: " << string(skey) << endl;
            lastkey = now_key; // 不应该是每16个是一组么：这里实现得比较巧妙
            block_offset += no_shared_length; // 跳过非共享内容

            if (now_key != string(skey)){
                block_offset += 8;
                block_offset += value_length;
                continue;
            }

            cout << "EXIST KEY!" << endl;

            // value type
            char value_type;
            fseek(fp,block_offset,SEEK_SET);
            fread(&value_type,sizeof(char),1,fp);
            if (value_type == 0){
                // cout << "NOT FOUND!!!!" << endl;
                // block_offset +=8;
                // block_offset += value_length;
                break;
            }

            // value
            block_offset += 8;
            char value[value_length+1];
            memset(value,'\0',sizeof(value)); //先全填充为'\0'
            fseek(fp,block_offset,SEEK_SET);
            fread(&value,sizeof(char),value_length,fp);
            cout << "value: " << value << endl << endl;
            found_flag = 1;
            block_offset += value_length;
            break;
        }
        if (found_flag == 0){
        cout << "Sorry, Not Found The KEY!" << endl;
        }
    }    

    /*************************** 读取 datablock ****************************/
    
    // 读取data_block
    /*
    for (int i=0;i < num_of_indexblock; i++){ // 处理每一个block
        cout << "Read data_block " << i << ":" <<endl;

        uint64_t block_offset = bx[i].offset, block_size = bx[i].size;

        // 获得当前块的limit offset
        uint64_t limit_offset = block_offset + block_size -1;
        char* start_number = new char();
        uint32_t num_of_restart = 0, k=0; // 重启点

        while(k < 4){
            fseek(fp,limit_offset,SEEK_SET);
            fread(start_number,sizeof(char),1,fp);
            num_of_restart = num_of_restart << 8|(uint32_t)*start_number;
            k++;
            limit_offset--;// 去掉restart count
        }
    
        cout << "    NUM of restart point: " << num_of_restart << endl;
        limit_offset -= num_of_restart * 4; // 去掉restart_offset

        fseek(fp,block_offset,SEEK_SET); // 注意这里是block的起始位置
        string lastkey = "";

        uint32_t record_i = 0;
        while(block_offset <= limit_offset){ // 处理每一条data_record
            cout << "    Read data_ecord " << record_i << ": \n" << endl;
            // 处理shared length
            char* length = new char[5];
            uint32_t shared_length, no_shared_length,value_length,bytes;
            fread(length,sizeof(char),5,fp);
            GetVarint32(length,5,&shared_length,bytes);
            block_offset += bytes;

            // 处理non_shared_key_length
            fseek(fp,block_offset,SEEK_SET);
            fread(length,sizeof(char),5,fp);
            GetVarint32(length,5,&no_shared_length,bytes);
            no_shared_length -= 8;
            block_offset += bytes;

            // 处理value_length
            fseek(fp,block_offset,SEEK_SET);
            fread(length,sizeof(char),5,fp);
            GetVarint32(length,5,&value_length,bytes);
            block_offset += bytes;

            // 打印
            cout << "    shared length: " << shared_length << endl 
                 << "    no shared length: " << no_shared_length << endl
                 << "    value length: " << value_length << endl;
        
            // 下面读取key
            char* key = new char[no_shared_length+1];
            memset(key,'\0',sizeof(key));

            fseek(fp,block_offset,SEEK_SET);
            fread(key,sizeof(char),no_shared_length,fp);
            string now_key;
            if (shared_length!=0){
                now_key = lastkey.substr(0,shared_length) + string(key);
            }
            else{
                now_key = key;
            }
        
            cout << "    key: " << now_key << endl;
            lastkey = now_key; // 不应该是每16个是一组么，这里没有?
            block_offset += no_shared_length; // 跳过非共享内容

            // 读取value_type
            char value_type;
            fseek(fp,block_offset,SEEK_SET);
            fread(&value_type,sizeof(char),1,fp);
            if (value_type == 0){
                cout << "    del\n";
            }
            else if (value_type == 1){
                cout << "    value_type" << endl;
            }
            block_offset += 8; 

            // 读取value
            char value[value_length+1];
            memset(value,'\0',sizeof(value)); //先全填充为'\0'
            fseek(fp,block_offset,SEEK_SET);
            fread(&value,sizeof(char),value_length,fp);
            cout << "    value: " << value << endl << endl;
            block_offset += value_length;

            record_i++;
        }
      */    
    //}
    return 0;
}

