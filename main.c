#include<string.h>
#include<stdio.h>
#include<assert.h>
#include<stdbool.h>
#include<stdint.h>

#include "extmem.h"

Buffer buf;

// Utils

#define RED "\033[31m"
#define NOCOLOR "\033[0m"
#define cprintf(fmt, ...) printf(RED fmt NOCOLOR, ##__VA_ARGS__)
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
void print_title(char *title) {
    int len = strlen(title) / 1.5;
    printf("\n%.*s\n", len, "--------------------------------------------------");
    cprintf("%s\n", title);
    printf("%.*s\n", len, "--------------------------------------------------");
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
void vector_push2_autosave(vector *v, int x1, int x2, int *disk) {
    vector_push_back(v, x1); vector_push_back(v, x2);
    if(v->size == v->capacity) {
        vector_save_and_free(v, (*disk)++, false);
        vector_init(v);
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
void display_quadruple_cbk(int cnt, int X, int Y) {
    int *info = (int*)user_data;
    if(info[0]) {
        printf("%d:\t%d\t%d\t%d\t%d\n", cnt/2, info[1], info[2], X, Y);
    } else {
        info[1] = X; info[2] = Y;
    }
    info[0] = !info[0];
}
void display_quadruple(int begin, int end) {
    int info[3] = {false, 0, 0};
    user_data = info;
    linear_scan(begin, end, display_quadruple_cbk, false);
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
    print_title("基于线性搜索的选择算法");
    buf.numIO = 0;

    vector v;
    vector_init(&v);
    user_data = &v;
    linear_scan(17, 49, task1_select_cbk, true);
    cprintf("满足选择条件的元组一共%d个\n", v.size / 2);

    vector_save_and_free(&v, 100, true);

    cprintf("IO读写一共%d次\n", (int)buf.numIO);
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
            vector_push2_autosave(&out, x1, y1, &to);
            p1++;
        } else {
            vector_push2_autosave(&out, x2, y2, &to);
            p2++;
        }
    }

    vector_free(&in1);
    vector_free(&in2);
    vector_free(&out);
}

void task2()
{
    print_title("两阶段多路归并排序算法");
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

    cprintf("IO读写一共%d次\n", (int)buf.numIO);
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
void indexed_select(int idx_blk, int end, int target, void (*callback)(int, int, int), bool verbose) {
    vector index;
    vector_init(&index);
    vector_load(&index, idx_blk, idx_blk + 1);

    for(int group=0; group < index.size/2; group++) {
        int minval = vector_get(&index, group * 2);
        int maxval = INT32_MAX;
        if(group + 1 < index.size / 2) {
            maxval = vector_get(&index, (group + 1) * 2);
        }
        if(target < minval || target > maxval) {
            continue;
        }
        int blkid = vector_get(&index, group * 2 + 1);
        int endid = blkid+5 < end ? blkid+5 : end;
        linear_scan(blkid, endid, callback, verbose);
    }
    vector_free(&index);
}
void task3()
{
    print_title("基于索引的关系选择算法");

    // make index for S
    make_index(317, 349, 350);

    // select from S
    buf.numIO = 0;
    int target = 107;
    vector result;
    vector_init(&result);
    user_data = &result;
    indexed_select(350, 349, target, task1_select_cbk, true);

    cprintf("满足选择条件的元组一共%d个\n", result.size / 2);
    vector_save_and_free(&result, 120, true);
    cprintf("IO读写一共%d次\n", (int)buf.numIO);
}

void task4_join_cbk(int cnt, int c, int d)
{
    void **info = user_data;
    vector *out = info[0]; int *to = info[1]; int a = *(int*)info[2]; int b = *(int*)info[3]; int *tot = info[4];
    if(c != a) return;
    vector_push2_autosave(out, a, b, to);
    vector_push2_autosave(out, c, d, to);
    ++*tot;
}
void task4()
{
    print_title("基于排序的连接操作算法");
    buf.numIO = 0;

    // on R.A = S.C
    int b1 = 301, e1 = 317, b2 = 317, e2 = 349;
    int to = 401, p1 = 0, p2 = 0, cnt = 0;
    vector in1, in2, out;
    vector_init(&in1); vector_init(&in2); vector_init(&out);

    for(;!(b1 == e1 && p1*2 == in1.size); ++p1) {
        if(b1 < e1 && p1*2 == in1.size) {
            vector_load(&in1, b1, b1+1);
            p1 = 0; ++b1;
        }
        int a = vector_get(&in1, p1*2), b = vector_get(&in1, p1*2+1);

        for(; !(b2 == e2 && p2*2 == in2.size); p2++) {
            if(b2 < e2 && p2*2 == in2.size) {
                vector_load(&in2, b2, b2+1);
                p2 = 0; ++b2;
            }
            if(vector_get(&in2, p2*2) >= a) {
                break;
            }
        }
        if(b2 == e2 && p2*2 == in2.size) {
            break;
        }
        int c = vector_get(&in2, p2*2), d = vector_get(&in2, p2*2+1);
        if(c != a) {
            continue;
        }

        bool many = true;
        for(int i=p2+1; i*2 < in2.size; ++i) {
            if(vector_get(&in2, i*2) != a) {
                many = false;
                break;
            }
        }
        if(many) {
            void* info[5] = {&out, &to, &a, &b, &cnt};
            user_data = info;
            indexed_select(350, 349, a, task4_join_cbk, false);
            continue;
        }

        for(int i=p2; i*2 < in2.size; ++i, ++cnt) {
            int c = vector_get(&in2, i*2), d = vector_get(&in2, i*2+1);
            if(c != a) break;
            vector_push2_autosave(&out, a, b, &to);
            vector_push2_autosave(&out, c, d, &to);
        }
    }
    
    vector_save_and_free(&out, to, false);
    vector_free(&in1); vector_free(&in2);
    printf("连接结果写入磁盘%d-%d\n", 401, to);
    cprintf("IO读写一共%d次\n", (int)buf.numIO);
    cprintf("连接结果一共%d个\n", cnt);
}

void task5_union() {
    print_title("基于排序的集合并运算");
    int b1 = 301, e1 = 317, b2 = 317, e2 = 349, to = 501;
    vector in1, in2, out;
    vector_init(&in1); vector_init(&in2); vector_init(&out);

    int x1, y1, x2, y2;
    int p1 = 0, p2 = 0, tot = 0;

    for(; !(b1 == e1 && p1 * 2 == in1.size) || !(b2 == e2 && p2 * 2 == in2.size); ++tot) {
        if(b1 < e1 && p1*2 == in1.size) {
            task2_load_blk(b1, b1+1, &in1);
            p1 = 0; ++b1;
        }
        if(b2 < e2 && p2*2 == in2.size) {
            task2_load_blk(b2, b2+1, &in2);
            p2 = 0; ++b2;
        }

        bool remain1, remain2;
        if((remain1 = (b1 != e1 || p1 * 2 != in1.size))) {
            x1 = vector_get(&in1, p1 * 2); 
            y1 = vector_get(&in1, p1 * 2 + 1);
        }
        if((remain2 = (b2 != e2 || p2 * 2 != in2.size))) {
            x2 = vector_get(&in2, p2 * 2);
            y2 = vector_get(&in2, p2 * 2 + 1);
        }

        if(remain1 && remain2) {
            if(x1 == x2 && y1 == y2) {
                vector_push2_autosave(&out, x1, y1, &to);
                p1++; p2++;
            } else if(tuple_cmp(x1, y1, x2, y2)) {
                vector_push2_autosave(&out, x1, y1, &to);
                p1++;
            } else {
                vector_push2_autosave(&out, x2, y2, &to);
                p2++;
            }
        } else if(remain1) {
            vector_push2_autosave(&out, x1, y1, &to);
            p1++;
        } else {
            vector_push2_autosave(&out, x2, y2, &to);
            p2++;
        }
    }

    vector_save_and_free(&out, to, false);
    vector_free(&in1); vector_free(&in2);
    printf("并集结果写入磁盘%d-%d\n", 501, to);
    cprintf("并集结果一共%d个\n", tot);
}
void task5_inter() {
    print_title("基于排序的集合交运算");
    int b1 = 301, e1 = 317, b2 = 317, e2 = 349, to = 601;
    vector in1, in2, out;
    vector_init(&in1); vector_init(&in2); vector_init(&out);

    int x1, y1, x2, y2;
    int p1 = 0, p2 = 0, tot = 0;

    for(; !(b1 == e1 && p1 * 2 == in1.size) && !(b2 == e2 && p2 * 2 == in2.size);) {
        if(b1 < e1 && p1*2 == in1.size) {
            task2_load_blk(b1, b1+1, &in1);
            p1 = 0; ++b1;
        }
        if(b2 < e2 && p2*2 == in2.size) {
            task2_load_blk(b2, b2+1, &in2);
            p2 = 0; ++b2;
        }

        x1 = vector_get(&in1, p1 * 2); y1 = vector_get(&in1, p1 * 2 + 1);
        x2 = vector_get(&in2, p2 * 2); y2 = vector_get(&in2, p2 * 2 + 1);

        if(x1 == x2 && y1 == y2) {
            vector_push2_autosave(&out, x1, y1, &to);
            p1++; p2++; tot++;
        } else if(tuple_cmp(x1, y1, x2, y2)) {
            p1++;
        } else {
            p2++;
        }
    }

    vector_save_and_free(&out, to, false);
    vector_free(&in1); vector_free(&in2);
    printf("交集结果写入磁盘%d-%d\n", 601, to);
    cprintf("交集结果一共%d个\n", tot);
}
void task5_diff() {
    print_title("基于排序的集合差运算");
    int b1 = 317, e1 = 349, b2 = 301, e2 = 317, to = 701;
    vector in1, in2, out;
    vector_init(&in1); vector_init(&in2); vector_init(&out);

    int x1, y1, x2, y2;
    int p1 = 0, p2 = 0, tot = 0;

    for(; !(b1 == e1 && p1 * 2 == in1.size) && !(b2 == e2 && p2 * 2 == in2.size);) {
        if(b1 < e1 && p1*2 == in1.size) {
            task2_load_blk(b1, b1+1, &in1);
            p1 = 0; ++b1;
        }
        if(b2 < e2 && p2*2 == in2.size) {
            task2_load_blk(b2, b2+1, &in2);
            p2 = 0; ++b2;
        }

        x1 = vector_get(&in1, p1 * 2); y1 = vector_get(&in1, p1 * 2 + 1);
        x2 = vector_get(&in2, p2 * 2); y2 = vector_get(&in2, p2 * 2 + 1);

        if(x1 == x2 && y1 == y2) {
            p1++; p2++;
        } else if(tuple_cmp(x1, y1, x2, y2)) {
            vector_push2_autosave(&out, x1, y1, &to);
            p1++; tot++;
        } else {
            p2++;
        }
    }

    for(; !(b1 == e1 && p1 * 2 == in1.size); ++p1, ++tot) {
        if(b1 < e1 && p1*2 == in1.size) {
            task2_load_blk(b1, b1+1, &in1);
            p1 = 0; ++b1;
        }
        x1 = vector_get(&in1, p1 * 2); y1 = vector_get(&in1, p1 * 2 + 1);
        vector_push2_autosave(&out, x1, y1, &to);
    }

    vector_save_and_free(&out, to, false);
    vector_free(&in1); vector_free(&in2);
    printf("差集结果写入磁盘%d-%d\n", 701, to);
    cprintf("差集结果一共%d个\n", tot);
}
void task5()
{

    // Union
    buf.numIO = 0;
    task5_union();
    cprintf("IO读写一共%d次\n", (int)buf.numIO);

    // Intersection
    buf.numIO = 0;
    task5_inter();
    cprintf("IO读写一共%d次\n", (int)buf.numIO);

    // Difference
    buf.numIO = 0;
    task5_diff();
    cprintf("IO读写一共%d次\n", (int)buf.numIO);
}

int main()
{
    initBuffer(520, 64, &buf);

    // printf("Display Table A:\n");
    // display(1, 17);
    // printf("\nDisplay Table B:\n");
    // display(17, 49);

    task1();
    // display(100, 102);
    // printf("\n");

    task2();
    // display(301, 317);
    // printf("\n");
    // display(317, 349);
    // printf("\n");

    task3();
    // display(120, 122);
    // printf("\n");

    task4();
    // printf("\nidx\tA\tB\tC\tD\n");
    // display_quadruple(401, 471);

    task5();
    // printf("\nUnion:\n");
    // display(501, 548);
    // printf("\nIntersection:\n");
    // display(601, 603);
    // printf("\n");
    // printf("\nDifference:\n");
    // display(701, 732);

    freeBuffer(&buf);
}