#include<string.h>
#include<stdio.h>
#include<assert.h>
#include<stdbool.h>
#include<stdint.h>

#include "extmem.h"

Buffer buf;

// Utils

typedef unsigned char* BLK;

bool is_null(unsigned char *p) {
    return *p == '\0';
}
int readint(unsigned char *p) {
    int res = 0;
    for (int i = 0; p[i] && i < 3; i++) {
        res = res * 10 + p[i] - '0';
    }
    assert(p[3] == '\0');
    return res;
}
void readtuple(unsigned char *p, int *x, int *y) {
    *x = readint(p);
    *y = readint(p + 4);
}

int at(BLK blk, int i) {
    return readint(blk + i * 4);
}
void writeint(BLK p, int pos, int x) {
    char buf[4];
    sprintf(buf, "%03d", x);
    memcpy(p + pos * 4, buf, 4);
}

// Data structures

// 基于缓冲区的Vector
#define BLK_CAP 14
typedef struct vector {
    int size, capacity;
    BLK head, tail;
} vector;
BLK* vector_nxt(BLK blk)
{
    return (void*)(blk + 7 * 8);
}
void vector_init(vector *v) {
    v->size = 0;
    v->capacity = BLK_CAP;
    v->head = getNewBlockInBuffer(&buf);
    assert(v->head);
    memset(v->head, 0x00, 64);
    v->tail = v->head;
}
void vector_extend(vector *v) {
    BLK blk = getNewBlockInBuffer(&buf);
    assert(blk);
    *vector_nxt(v->tail) = blk;
    v->tail = blk;
    memset(blk, 0x00, 64);
    v->capacity += BLK_CAP;
}
int vector_get(vector *v, int idx)
{
    assert(idx < v->size);
    int blkid = idx / BLK_CAP;
    BLK blk = v->head;
    for (int i = 0; i < blkid; i++) {
        blk = *vector_nxt(blk);
    }
    return at(blk, idx % BLK_CAP);
}
int vector_set(vector *v, int idx, int x)
{
    assert(idx < v->size);
    int blkid = idx / BLK_CAP;
    BLK blk = v->head;
    for (int i = 0; i < blkid; i++) {
        blk = *vector_nxt(blk);
    }
    int old = at(blk, idx % BLK_CAP);
    writeint(blk, idx % BLK_CAP, x);
    return old;
}
void vector_push_back(vector *v, int x) {
    if (v->size == v->capacity) {
        vector_extend(v);
    }
    writeint(v->tail, v->size % BLK_CAP, x);
    v->size++;
}
void vector_free(vector *v) {
    if(!v->head) return;
    BLK blk = v->head;
    while (blk) {
        BLK nxt = *vector_nxt(blk);
        freeBlockInBuffer(blk, &buf);
        blk = nxt;
    }
    v->head = v->tail = NULL;
    v->size = v->capacity = 0;
}
void vector_swap(vector *v, int i, int j) {
    if(i == j) return;
    int val1 = vector_get(v, i);
    int val2 = vector_set(v, j, val1);
    vector_set(v, i, val2);
}
// save a vector to disk, also free used blocks
// this behavior due to the extmem.h interface, `writeBlockToDisk` frees the block
void vector_save_and_free(vector *v, int disk, bool verbose) {
    assert(disk < 1000);
    BLK blk = v->head;
    while(blk) {
        BLK nxt = *vector_nxt(blk);
        memset(blk + 7*8, 0x00, 8);
        sprintf((char*)blk + 7*8, "%03d", disk + 1);
        if(verbose) printf("注：结果写入磁盘：%d\n", disk);
        assert(!writeBlockToDisk(blk, disk++, &buf));
        blk = nxt;
    }
}
// load a vector directly from disk, without any redundant buffer usage
void vector_load(vector *v, int begin, int end) {
    vector_free(v);
    BLK blk = readBlockFromDisk(begin, &buf);
    assert(blk);
    v->capacity = BLK_CAP;
    v->head = blk;
    v->tail = blk;
    *vector_nxt(v->tail) = NULL;
    for(int i=begin+1; i < end; i++) {
        blk = readBlockFromDisk(i, &buf);
        assert(blk);
        *vector_nxt(v->tail) = blk;
        v->tail = blk;
        *vector_nxt(v->tail) = NULL;
        v->capacity += BLK_CAP;
    }
    v->size = -1;
    for(int i=BLK_CAP-1; i>=0; --i) {
        if(!is_null(v->tail + i*4)) {
            v->size = v->capacity + i - BLK_CAP + 1;
            break;
        }
    }
    assert(v->size != -1);
}
void vector_print(vector *v) {
    printf("Vector: size=%d, capacity=%d\n", v->size, v->capacity);
    for(int i=0; i < v->size; i++) {
        printf("%d ", vector_get(v, i));
    }
    printf("\n");
}


// Core functions

void *user_data=NULL;
void linear_scan(int begin, int end, void (*callback)(int, int, int), bool verbose)
{
    int X, Y;
    int cnt = 0;
    for (int blkid = begin; blkid != 0 && blkid != end;)
    {
        if(verbose) {
            printf("读入数据库块%d\n", blkid);
        }
        BLK blk = readBlockFromDisk(blkid, &buf);
        assert(blk);
        for (int i = 0; i < 7; i++)
        {
            if(is_null(blk + i * 8)) {
                break;
            }
            readtuple(blk + i * 8, &X, &Y);
            callback(++cnt, X, Y);
        }
        blkid = readint(blk + 7 * 8);
        freeBlockInBuffer(blk, &buf);
    }
}


// Application

void print_tuple_cbk(int cnt, int X, int Y)
{
    printf("%d: (%d, %d) \n", cnt, X, Y);
}
void display(int first_blk, int end_blk)
{
    linear_scan(first_blk, end_blk, print_tuple_cbk, false);
}

void task1_select_cbk(int cnt, int X, int Y)
{
    if (X == 107)
    {
        printf("(X=%d, Y=%d)\n", X, Y);
        vector_push_back((vector*)user_data, X);
        vector_push_back((vector*)user_data, Y);
    }
}
void task1()
{
    printf("---------------------------\n");
    printf("基于线性搜索的选择算法 S.C=107:\n");
    printf("---------------------------\n");
    buf.numIO = 0;

    vector v;
    vector_init(&v);
    user_data = &v;
    linear_scan(17, 49, task1_select_cbk, true);
    printf("满足选择条件的元组一共%d个\n", v.size / 2);

    vector_save_and_free(&v, 100, true);

    printf("IO读写一共%d次\n", (int)buf.numIO);
}

void insert_cbk(int cnt, int X, int Y)
{
    vector_push_back((vector*)user_data, X);
    vector_push_back((vector*)user_data, Y);
}
void sort(int begin, int end, int to)
{
    vector v;
    vector_init(&v);
    user_data = &v;
    vector_load(&v, begin, end);

    int tuple_num = v.size / 2;
    for(int i=0; i < tuple_num; i++) {
        for(int j=i+1; j < tuple_num; j++) {
            int a1 = vector_get(&v, i*2), a2 = vector_get(&v, j*2);
            if(a1 > a2) {
                vector_swap(&v, i*2, j*2);
                vector_swap(&v, i*2+1, j*2+1);
            } else if (a1 == a2) {
                int b1 = vector_get(&v, i*2+1), b2 = vector_get(&v, j*2+1);
                if(b1 > b2) {
                    vector_swap(&v, i*2+1, j*2+1); // a is the same, swap b
                }
            }
        }
    }

    vector_save_and_free(&v, to, false);
}

bool tuple_cmp(int a1, int b1, int a2, int b2)
{
    if(a1 < a2) return true;
    if(a1 > a2) return false;
    return b1 < b2;
}
void task2_load_blk(int b, int e, vector *in) {
    vector_free(in); vector_init(in);
    user_data = in;
    linear_scan(b, b + 1, insert_cbk, false);
}
void merge(int b1, int e1, int b2, int e2, int to) {
    vector in1, in2, out;
    vector_init(&in1); vector_init(&in2); vector_init(&out);

    int x1, y1, x2, y2;
    int p1 = 0, p2 = 0;

    while(!(b1 == e1 && p1 * 2 == in1.size) || !(b2 == e2 && p2 * 2 == in2.size)) {
        if(b1 < e1 && p1*2 == in1.size) {
            task2_load_blk(b1, b1+1, &in1);
            p1 = 0; ++b1;
        }
        if(b2 < e2 && p2*2 == in2.size) {
            task2_load_blk(b2, b2+1, &in2);
            p2 = 0; ++b2;
        }

        if(b1 != e1 || p1 * 2 != in1.size) {
            x1 = vector_get(&in1, p1 * 2); 
            y1 = vector_get(&in1, p1 * 2 + 1);
        }
        if(b2 != e2 || p2 * 2 != in2.size) {
            x2 = vector_get(&in2, p2 * 2);
            y2 = vector_get(&in2, p2 * 2 + 1);
        }

        bool from_in1 = (b2 == e2 && p2*2 == in2.size) || // in2 is empty
            (!(b1 == e1 && p1*2 == in1.size) && tuple_cmp(x1, y1, x2, y2)); // in1 is not empty, and in1 < in2

        if(from_in1) {
            vector_push_back(&out, x1);
            vector_push_back(&out, y1);
            p1++;
        } else {
            vector_push_back(&out, x2);
            vector_push_back(&out, y2);
            p2++;
        }
        if(out.size == out.capacity) {
            vector_save_and_free(&out, to++, false);
            vector_init(&out);
        }
    }

    vector_free(&in1);
    vector_free(&in2);
    vector_free(&out);
}

void task2()
{
    printf("---------------------------\n");
    printf("两阶段多路归并排序算法（TPMMS）\n");
    printf("---------------------------\n");
    buf.numIO = 0;

    // sort R
    sort(1, 9, 201);
    sort(9, 17, 209);
    merge(201, 209, 209, 217, 301);

    // sort S
    for(int i=17; i < 49; i+=8) {
        sort(i, i+8, 200 + i);
    }
    merge(217, 225, 225, 233, 251);
    merge(233, 241, 241, 249, 267);
    merge(251, 267, 267, 283, 317);

    printf("IO读写一共%d次\n", (int)buf.numIO);
}

void task3_getmin_cbk(int cnt, int X, int Y)
{
    int *minval = (int*)user_data;
    *minval = *minval < X ? *minval : X;
}
void make_index(int begin, int end, int to) {
    vector index;
    vector_init(&index);

    for(int i=begin; i < end; i+=5) {
        int group = (i - begin) / 5;
        int minval = 0x7fffffff;
        user_data = &minval;
        linear_scan(i, i+5 < end ? i+5 : end, task3_getmin_cbk, false);

        vector_push_back(&index, minval); // min value
        vector_push_back(&index, i); // block pointer
    }
    vector_save_and_free(&index, to, false);
}
void task3()
{
    printf("----------------------------\n");
    printf("基于索引的关系选择算法（S.C=107）\n");
    printf("----------------------------\n");

    // make index for S
    make_index(317, 349, 350);

    // select from S
    buf.numIO = 0;
    int target = 107;
    vector index;
    vector_init(&index);
    vector_load(&index, 350, 351);

    vector result;
    vector_init(&result);
    for(int group=0; group < index.size / 2; group++) {
        int minval = vector_get(&index, group * 2);
        int maxval = INT32_MAX;
        if(group + 1 < index.size / 2) {
            maxval = vector_get(&index, (group + 1) * 2);
        }
        if(target < minval || target > maxval) {
            continue;
        }

        int blkid = vector_get(&index, group * 2 + 1);
        user_data = &result;
        linear_scan(blkid, blkid + 5, task1_select_cbk, true);
    }

    printf("满足选择条件的元组一共%d个\n", result.size / 2);
    vector_save_and_free(&result, 120, true);
    printf("IO读写一共%d次\n", (int)buf.numIO);
}

void task4()
{
    printf("----------------------------\n");
    printf("基于排序的连接操作算法（Sort-Merge-Join）\n");
    printf("----------------------------\n");
    buf.numIO = 0;

    // on R.A = S.C
    int b1 = 301, e1 = 317, b2 = 317, e2 = 349;
    int to = 401;
}

int main()
{
    initBuffer(520, 64, &buf);

    // printf("Display Table A:\n");
    // display(1, 17);
    // printf("\nDisplay Table B:\n");
    // display(17, 49);

    task1();
    display(100, 102);
    printf("\n");

    task2();
    // display(301, 317);
    // printf("\n");
    // display(317, 349);
    // printf("\n");

    task3();
    display(120, 122);
    printf("\n");

    freeBuffer(&buf);
}