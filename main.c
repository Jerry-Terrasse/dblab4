#include<string.h>
#include<stdio.h>
#include<assert.h>
#include<stdbool.h>
#include<stdint.h>

#include "extmem.h"

Buffer buf;

// Utils

int readint(char *p) {
    int res = 0;
    for (int i = 0; p[i] && i < 3; i++) {
        res = res * 10 + p[i] - '0';
    }
    assert(p[3] == '\0');
    return res;
}
void readtuple(char *p, int *x, int *y) {
    *x = readint(p);
    *y = readint(p + 4);
}

int at(char *blk, int i) {
    return readint(blk + i * 4);
}
void writeint(char *p, int pos, int x) {
    char buf[4];
    sprintf(buf, "%03d", x);
    memcpy(p + pos * 4, buf, 4);
}

// Data structures

// 基于缓冲区的Vector
#define BLK_CAP 14
typedef struct vector {
    int size, capacity;
    char *head, *tail;
} vector;
char** vector_nxt(char *blk)
{
    return (char**)(blk + 7 * 8);
}
void vector_init(vector *v) {
    v->size = 0;
    v->capacity = BLK_CAP;
    v->head = getNewBlockInBuffer(&buf);
    assert(v->head);
    v->tail = v->head;
    *vector_nxt(v->head) = NULL;
}
void vector_extend(vector *v) {
    char *blk = getNewBlockInBuffer(&buf);
    assert(blk);
    *vector_nxt(v->tail) = blk;
    v->tail = blk;
    *vector_nxt(v->tail) = NULL;
    v->capacity += BLK_CAP;
}
int vector_get(vector *v, int idx)
{
    assert(idx < v->size);
    int blkid = idx / BLK_CAP;
    char *blk = v->head;
    for (int i = 0; i < blkid; i++) {
        blk = *vector_nxt(blk);
    }
    return at(blk, idx % BLK_CAP);
}
int vector_set(vector *v, int idx, int x)
{
    assert(idx < v->size);
    int blkid = idx / BLK_CAP;
    char *blk = v->head;
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
    char *blk = v->head;
    while (blk) {
        char *nxt = *vector_nxt(blk);
        freeBlockInBuffer(blk, &buf);
        blk = nxt;
    }
}
void vector_swap(vector *v, int i, int j) {
    if(i == j) return;
    int val1 = vector_get(v, i);
    int val2 = vector_set(v, j, val1);
    vector_set(v, i, val2);
}
void vector_save(vector *v, int disk, bool verbose) {
    assert(disk < 1000);
    for(int i=0; i < v->size; i+=BLK_CAP) {
        char *blk = getNewBlockInBuffer(&buf);
        assert(blk);
        memset(blk, 0x00, 64);
        for(int j=0; j < BLK_CAP && i+j < v->size; j++) {
            writeint(blk, j, vector_get(v, i+j));
        }
        sprintf(blk + 7*8, "%03d", disk + 1);
        if(verbose) printf("注：结果写入磁盘：%d\n", disk);
        assert(!writeBlockToDisk(blk, disk, &buf));
        disk ++;
    }
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
        char *blk = readBlockFromDisk(blkid, &buf);
        assert(blk);
        for (int i = 0; i < 7; i++)
        {
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

    vector_save(&v, 100, true);
    vector_free(&v);

    printf("IO读写一共%d次\n", buf.numIO);
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
    linear_scan(begin, end, insert_cbk, false);

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

    vector_save(&v, to, false);
    vector_free(&v);
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
            task2_load_blk(b1++, b1, &in1);
            p1 = 0;
        }
        if(b2 < e2 && p2*2 == in2.size) {
            task2_load_blk(b2++, b2, &in2);
            p2 = 0;
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
            vector_save(&out, to++, false);
            vector_free(&out); vector_init(&out);
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

    printf("IO读写一共%d次\n", buf.numIO);
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
    display(301, 317);
    printf("\n");
    display(317, 349);
    printf("\n");

    freeBuffer(&buf);
}