/* ==========================================================================
 * rdma.c - support RDMA protocol for transport layer.
 * --------------------------------------------------------------------------
 * Copyright (C) 2021  zhenwei pi
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to permit
 * persons to whom the Software is furnished to do so, subject to the
 * following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
 * NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
 * OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
 * USE OR OTHER DEALINGS IN THE SOFTWARE.
 * ==========================================================================
 */

#include "server.h"
#include "connection.h"
#include "connhelpers.h"

static void serverNetError(char *err, const char *fmt, ...)
{
    va_list ap;

    if (!err)
        return;
    va_start(ap, fmt);
    vsnprintf(err, ANET_ERR_LEN, fmt, ap);
    va_end(ap);
}

// todo
#define USE_RDMA

#ifdef USE_RDMA
#ifdef __linux__ /* currently RDMA is only supported on Linux */
#include <assert.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <rdma/rdma_cma.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/mman.h>

#define MIN(a, b) (a) < (b) ? a : b
#define REDIS_MAX_SGE 128
#define REDIS_RDMA_SERVER_RX_SIZE 9000
#define REDIS_RDMA_SERVER_TX_SIZE 9000
// todo, have a look up table for different redis message size
// #define REDIS_RDMA_SERVER_RX_SET_SIZE 1069
// #define REDIS_RDMA_SERVER_RX_GET_SIZE 36
// #define REDIS_RDMA_SERVER_TX_ACK_SIZE 5
// #define REDIS_RDMA_SERVER_TX_GET_SIZE 1033

// #define REDIS_SYNCIO_RES 10

typedef struct rdma_connection
{
    connection c;
    struct rdma_cm_id *cm_id;
    int last_errno;
} rdma_connection;

typedef struct RdmaContext
{
    connection *conn;
    char *ip;
    int port;
    struct ibv_pd *pd;
    struct rdma_event_channel *cm_channel;
    struct ibv_cq *send_cq;
    struct ibv_cq *recv_cq;
    long long timeEvent;

    /* TX */
    char *send_buf;
    uint32_t send_length;
    struct ibv_mr *send_mr;

    bool *send_status;
    struct ibv_mr *status_mr;

    /* RX */
    char *recv_buf;
    uint32_t recv_length;
    uint32_t recv_offset;
    uint32_t outstanding_msg_size;
    struct ibv_mr *recv_mr;

} RdmaContext;

// note: listen channel -> cm_id -> rdma_context -> connection
static struct rdma_event_channel *listen_channel;
static struct rdma_cm_id *listen_cmids[CONFIG_BINDADDR_MAX];

// manually specify the ib device
char ib_devname_server[100];
static struct ibv_device* find_ib_device(const char *ib_devname)
{
    int num_devices;
    struct ibv_device **dev_list;
    struct ibv_device *ib_dev = NULL;

    dev_list = ibv_get_device_list(&num_devices);
    assert(num_devices > 0);
    for (int i = 0; i < num_devices; i++)
    {
        if (strcmp(ibv_get_device_name(dev_list[i]), ib_devname) == 0)
        {
            ib_dev = dev_list[i];
            break;
        }
    }
    assert(ib_dev != NULL);
    return ib_dev;
}


static size_t rdmaPostSend(RdmaContext *ctx, struct rdma_cm_id *cm_id, const void *data, size_t data_len)
{
    struct ibv_send_wr send_wr, *bad_wr;
    struct ibv_sge sge;
    uint32_t index;
    uint64_t addr;

    /* find an unused send buffer entry */
    // todo, is this time consuming?
    for (index = 0; index < REDIS_MAX_SGE; index++)
    {
        if (!ctx->send_status[index])
        {
            break;
        }
    }

    // not likely to fill up the send buffer
    assert(index < REDIS_MAX_SGE);
    assert(data_len <= REDIS_RDMA_SERVER_TX_SIZE);

    ctx->send_status[index] = true;
    addr = ctx->send_buf + index * REDIS_RDMA_SERVER_TX_SIZE;
    memcpy(addr, data, data_len);

    sge.addr = (uint64_t)addr;
    sge.lkey = ctx->send_mr->lkey;

    // assert(data_len == REDIS_RDMA_SERVER_TX_ACK_SIZE || data_len == REDIS_RDMA_SERVER_TX_GET_SIZE);
    sge.length = data_len;

    send_wr.sg_list = &sge;
    send_wr.num_sge = 1;
    send_wr.wr_id = index;
    send_wr.opcode = IBV_WR_SEND;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    send_wr.next = NULL;
    serverLog(LL_DEBUG, "post send, length: %d", data_len);
    if (ibv_post_send(cm_id->qp, &send_wr, &bad_wr))
    {
        return C_ERR;
    }

    return data_len;
}

static int rdmaPostRecv(RdmaContext *ctx, struct rdma_cm_id *cm_id, uint32_t index, size_t data_len)
{
    struct ibv_sge sge;
    struct ibv_recv_wr recv_wr, *bad_wr;

    sge.addr = (uint64_t)(ctx->recv_buf + index * REDIS_RDMA_SERVER_RX_SIZE);
    sge.length = data_len;
    sge.lkey = ctx->recv_mr->lkey;

    recv_wr.wr_id = index;
    recv_wr.sg_list = &sge;
    recv_wr.num_sge = 1;
    recv_wr.next = NULL;

    if (ibv_post_recv(cm_id->qp, &recv_wr, &bad_wr))
    {
        return C_ERR;
    }

    return C_OK;
}

static void rdmaDestroyIoBuf(RdmaContext *ctx)
{
    if (ctx->recv_mr)
    {
        ibv_dereg_mr(ctx->recv_mr);
        ctx->recv_mr = NULL;
    }

    munmap(ctx->recv_buf, REDIS_MAX_SGE * REDIS_RDMA_SERVER_RX_SIZE);
    ctx->recv_buf = NULL;

    if (ctx->send_mr)
    {
        ibv_dereg_mr(ctx->send_mr);
        ctx->send_mr = NULL;
    }

    munmap(ctx->send_buf, REDIS_MAX_SGE * REDIS_RDMA_SERVER_TX_SIZE);
    ctx->send_buf = NULL;

    if (ctx->status_mr)
    {
        ibv_dereg_mr(ctx->status_mr);
        ctx->status_mr = NULL;
    }

    munmap(ctx->send_status, REDIS_MAX_SGE * sizeof(bool));
    ctx->send_status = NULL;
}

static int rdmaSetupIoBuf(RdmaContext *ctx, struct rdma_cm_id *cm_id)
{
    int access = IBV_ACCESS_LOCAL_WRITE;
    size_t length = REDIS_MAX_SGE * sizeof(bool);
    ctx->send_status = mmap(NULL, length, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    ctx->status_mr = ibv_reg_mr(ctx->pd, ctx->send_status, length, access);
    if (!ctx->status_mr)
    {
        serverLog(LL_WARNING, "RDMA: reg mr for status failed");
        goto destroy_iobuf;
    }

    // access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
    length = REDIS_MAX_SGE * REDIS_RDMA_SERVER_RX_SIZE;
    ctx->recv_buf = mmap(NULL, length, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    ctx->recv_length = length;
    ctx->recv_offset = 0;
    ctx->outstanding_msg_size = 0;
    ctx->recv_mr = ibv_reg_mr(ctx->pd, ctx->recv_buf, length, access);
    if (!ctx->recv_mr)
    {
        serverLog(LL_WARNING, "RDMA: reg mr for recv buffer failed");
        goto destroy_iobuf;
    }

    // for (int i = 0; i < REDIS_MAX_SGE; i++)
    // {
    //     if (rdmaPostRecv(ctx, cm_id, i) == C_ERR)
    //     {
    //         serverLog(LL_WARNING, "RDMA: post recv failed");
    //         goto destroy_iobuf;
    //     }
    // }

    length = REDIS_MAX_SGE * REDIS_RDMA_SERVER_TX_SIZE;
    ctx->send_buf = mmap(NULL, length, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    ctx->send_length = length;
    ctx->send_mr = ibv_reg_mr(ctx->pd, ctx->send_buf, length, access);
    if (!ctx->send_mr)
    {
        serverLog(LL_WARNING, "RDMA: reg mr for send buffer failed");
        goto destroy_iobuf;
    }

    return C_OK;

destroy_iobuf:
    rdmaDestroyIoBuf(ctx);
    return C_ERR;
}

// not used
static int rdmaHandleEstablished(struct rdma_cm_event *ev)
{
    // struct rdma_cm_id *cm_id = ev->id;
    // RdmaContext *ctx = cm_id->context;

    return C_OK;
}

static int rdmaHandleDisconnect(struct rdma_cm_event *ev)
{
    struct rdma_cm_id *cm_id = ev->id;
    RdmaContext *ctx = cm_id->context;
    connection *conn = ctx->conn;

    conn->state = CONN_STATE_CLOSED;

    /* kick connection read/write handler to avoid resource leak */
    if (conn->read_handler)
    {
        callHandler(conn, conn->read_handler);
    }
    else if (conn->write_handler)
    {
        callHandler(conn, conn->write_handler);
    }

    return C_OK;
}

static int connRdmaHandleSend(RdmaContext *ctx, uint32_t index)
{
    ctx->send_status[index] = false;

    return C_OK;
}

static int connRdmaHandleRecv(RdmaContext *ctx, struct rdma_cm_id *cm_id, uint32_t index, uint32_t byte_len)
{
    assert(byte_len > 0);
    assert(byte_len <= REDIS_RDMA_SERVER_RX_SIZE);
    ctx->recv_offset = index * REDIS_RDMA_SERVER_RX_SIZE;
    ctx->outstanding_msg_size = byte_len;

    if (ctx->conn->read_handler && (callHandler(ctx->conn, ctx->conn->read_handler) == C_ERR))
    {
        return C_ERR;
    }

    // to replenish the recv buffer
    // assert(byte_len == REDIS_RDMA_SERVER_RX_SET_SIZE || byte_len == REDIS_RDMA_SERVER_RX_GET_SIZE);
    return rdmaPostRecv(ctx, cm_id, index, byte_len);
}

static int connRdmaHandleCq(rdma_connection *rdma_conn, bool rx)
{
    // serverLog(LL_VERBOSE, "RDMA: cq handle\n");
    struct rdma_cm_id *cm_id = rdma_conn->cm_id;
    RdmaContext *ctx = cm_id->context;
    struct ibv_wc wc = {0};
    int ret;

pollcq:
    if (rx)
    {
        ret = ibv_poll_cq(ctx->recv_cq, 1, &wc);
    }
    else
    {
        ret = ibv_poll_cq(ctx->send_cq, 1, &wc);
    }
    if (ret < 0)
    {
        serverLog(LL_WARNING, "RDMA: poll recv CQ error");
        return C_ERR;
    }
    else if (ret == 0)
    {
        return C_OK;
    }

    if (wc.status != IBV_WC_SUCCESS)
    {
        serverLog(LL_WARNING, "RDMA: CQ handle error status 0x%x", wc.status);
        return C_ERR;
    }

    switch (wc.opcode)
    {
    case IBV_WC_RECV:
        if (connRdmaHandleRecv(ctx, cm_id, wc.wr_id, wc.byte_len) == C_ERR)
        {
            return C_ERR;
        }
        break;

    case IBV_WC_SEND:
        if (connRdmaHandleSend(ctx, wc.wr_id) == C_ERR)
        {
            return C_ERR;
        }

        break;

    default:
        serverLog(LL_WARNING, "RDMA: unexpected opcode 0x[%x]", wc.opcode);
        return C_ERR;
    }

    goto pollcq;
}

static int connRdmaAccept(connection *conn, ConnectionCallbackFunc accept_handler)
{
    int ret = C_OK;

    if (conn->state != CONN_STATE_ACCEPTING)
        return C_ERR;

    conn->state = CONN_STATE_CONNECTED;

    connIncrRefs(conn);
    if (!callHandler(conn, accept_handler))
        ret = C_ERR;
    connDecrRefs(conn);

    return ret;
}

void connRdmaEventHandler(struct aeEventLoop *el, int fd, void *clientData, int mask)
{
    // serverLog(LL_VERBOSE, "RDMA: connRdmaEventHandler\n");
    rdma_connection *rdma_conn = (rdma_connection *)clientData;
    connection *conn = &rdma_conn->c;
    struct rdma_cm_id *cm_id = rdma_conn->cm_id;
    RdmaContext *ctx = cm_id->context;

    UNUSED(el);
    UNUSED(fd);
    UNUSED(mask);

    if (connRdmaHandleCq(rdma_conn, true) == C_ERR || connRdmaHandleCq(rdma_conn, false) == C_ERR)
    {
        conn->state = CONN_STATE_ERROR;
        return;
    }

    if (conn->write_handler)
    {
        callHandler(conn, conn->write_handler);
    }
}

static int connRdmaSetRwHandler(connection *conn)
{
    rdma_connection *rdma_conn = (rdma_connection *)conn;
    struct rdma_cm_id *cm_id = rdma_conn->cm_id;
    RdmaContext *ctx = cm_id->context;

    /* save conn into RdmaContext */
    ctx->conn = conn;

    return C_OK;
}

static int connRdmaSetWriteHandler(connection *conn, ConnectionCallbackFunc func, int barrier)
{

    conn->write_handler = func;
    if (barrier)
    {
        conn->flags |= CONN_FLAG_WRITE_BARRIER;
    }
    else
    {
        conn->flags &= ~CONN_FLAG_WRITE_BARRIER;
    }

    return connRdmaSetRwHandler(conn);
}

static int connRdmaSetReadHandler(connection *conn, ConnectionCallbackFunc func)
{
    conn->read_handler = func;

    return connRdmaSetRwHandler(conn);
}

static const char *connRdmaGetLastError(connection *conn)
{
    return strerror(conn->last_errno);
}

static inline void rdmaConnectFailed(rdma_connection *rdma_conn)
{
    connection *conn = &rdma_conn->c;

    conn->state = CONN_STATE_ERROR;
    conn->last_errno = ENETUNREACH;
}

static int rdmaCreateResource(RdmaContext *ctx, struct rdma_cm_id *cm_id)
{
    int ret = C_OK;
    struct ibv_qp_init_attr init_attr;
    // struct ibv_comp_channel *comp_channel = NULL;
    struct ibv_cq *send_cq = NULL;
    struct ibv_cq *recv_cq = NULL;
    struct ibv_pd *pd = NULL;

    struct ibv_context *ib_ctx = NULL;
    if (ib_devname_server[0] == 0)
    {
        ib_ctx = cm_id->verbs;
    } else {
        struct ibv_device *ib_dev = find_ib_device(ib_devname_server);
        ib_ctx = ibv_open_device(ib_dev);
        printf("RDMA: using ib device %s\n", ib_devname_server);
        assert(ib_ctx != NULL);
    }
    pd = ibv_alloc_pd(ib_ctx);
    if (!pd)
    {
        serverLog(LL_WARNING, "RDMA: ibv alloc pd failed");
        return C_ERR;
    }

    ctx->pd = pd;

    // comp_channel = ibv_create_comp_channel(cm_id->verbs);
    // if (!comp_channel)
    // {
    //     serverLog(LL_WARNING, "RDMA: ibv create comp channel failed");
    //     return C_ERR;
    // }

    // ctx->comp_channel = comp_channel;

    send_cq = ibv_create_cq(ib_ctx, REDIS_MAX_SGE, NULL, NULL, 0);
    if (!send_cq)
    {
        serverLog(LL_WARNING, "RDMA: ibv create send cq failed");
        return C_ERR;
    }

    recv_cq = ibv_create_cq(ib_ctx, REDIS_MAX_SGE, NULL, NULL, 0);
    if (!recv_cq)
    {
        serverLog(LL_WARNING, "RDMA: ibv create recv cq failed");
        return C_ERR;
    }

    ctx->send_cq = send_cq;
    ctx->recv_cq = recv_cq;

    memset(&init_attr, 0, sizeof(init_attr));
    init_attr.cap.max_send_wr = REDIS_MAX_SGE;
    init_attr.cap.max_recv_wr = REDIS_MAX_SGE;
    init_attr.cap.max_send_sge = 1;
    init_attr.cap.max_recv_sge = 1;
    init_attr.qp_type = IBV_QPT_RC;
    init_attr.send_cq = send_cq;
    init_attr.recv_cq = recv_cq;
    init_attr.srq = NULL;

    cm_id->qp = ibv_create_qp(pd, &init_attr);
    if (cm_id->qp == NULL)
    // ret = rdma_create_qp(cm_id, pd, &init_attr);
    // if (ret)
    {
        serverLog(LL_WARNING, "RDMA: create qp failed");
        return C_ERR;
    }

    // required by sysb to change the states of qp
    int flags;
    struct ibv_qp_attr attr = {0};
	attr.pkey_index = 0;
	attr.port_num = 1;
	attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE;
	attr.qp_state = IBV_QPS_INIT;
	flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
	assert (!ibv_modify_qp(cm_id->qp, &attr, flags));

    if (rdmaSetupIoBuf(ctx, cm_id))
    {
        return C_ERR;
    }

    return C_OK;
}

static void rdmaReleaseResource(RdmaContext *ctx)
{
    rdmaDestroyIoBuf(ctx);

    if (ctx->send_cq)
    {
        ibv_destroy_cq(ctx->send_cq);
    }

    if (ctx->recv_cq)
    {
        ibv_destroy_cq(ctx->recv_cq);
    }

    if (ctx->pd)
    {
        ibv_dealloc_pd(ctx->pd);
    }
}

static void connRdmaClose(connection *conn)
{
    serverLog(LL_VERBOSE, "RDMA: close connection %p", conn);
    rdma_connection *rdma_conn = (rdma_connection *)conn;
    struct rdma_cm_id *cm_id = rdma_conn->cm_id;
    RdmaContext *ctx;

    if (conn->fd != -1)
    {
        aeDeleteFileEvent(server.el, conn->fd, AE_READABLE);
        conn->fd = -1;
    }

    if (!cm_id)
    {
        return;
    }

    ctx = cm_id->context;
    if (ctx->timeEvent > 0)
    {
        aeDeleteTimeEvent(server.el, ctx->timeEvent);
    }

    rdma_disconnect(cm_id);

    /* poll all CQ before close */
    connRdmaHandleCq(rdma_conn, true);
    connRdmaHandleCq(rdma_conn, false);
    rdmaReleaseResource(ctx);
    if (cm_id->qp)
    {
        ibv_destroy_qp(cm_id->qp);
    }

    rdma_destroy_id(cm_id);
    if (ctx->cm_channel)
    {
        aeDeleteFileEvent(server.el, ctx->cm_channel->fd, AE_READABLE);
        rdma_destroy_event_channel(ctx->cm_channel);
    }

    rdma_conn->cm_id = NULL;
    zfree(ctx);
    zfree(conn);
}

static int connRdmaWrite(connection *conn, const void *data, size_t data_len)
{
    rdma_connection *rdma_conn = (rdma_connection *)conn;
    struct rdma_cm_id *cm_id = rdma_conn->cm_id;
    RdmaContext *ctx = cm_id->context;
    // uint32_t towrite;

    if (conn->state == CONN_STATE_ERROR || conn->state == CONN_STATE_CLOSED)
    {
        return C_ERR;
    }

    return rdmaPostSend(ctx, cm_id, data, data_len);
}

static inline uint32_t rdmaRead(RdmaContext *ctx, void *buf, size_t buf_len)
{
    uint32_t toread = MIN(ctx->outstanding_msg_size, buf_len);
    memcpy(buf, ctx->recv_buf + ctx->recv_offset, toread);

    ctx->recv_offset = 0;
    ctx->outstanding_msg_size = 0;

    return toread;
}

static int connRdmaRead(connection *conn, void *buf, size_t buf_len)
{
    rdma_connection *rdma_conn = (rdma_connection *)conn;
    struct rdma_cm_id *cm_id = rdma_conn->cm_id;
    RdmaContext *ctx = cm_id->context;
    // serverLog(LL_DEBUG, "connection state: %d while reading data", conn->state);

    if (conn->state == CONN_STATE_ERROR || conn->state == CONN_STATE_CLOSED)
    {
        return C_ERR;
    }

    // make sure the message is ready
    assert(ctx->outstanding_msg_size != 0);

    return rdmaRead(ctx, buf, buf_len);
}

static int connRdmaGetType(connection *conn)
{
    UNUSED(conn);

    return CONN_TYPE_RDMA;
}

ConnectionType CT_RDMA = {
    .ae_handler = connRdmaEventHandler,
    .accept = connRdmaAccept,
    .set_read_handler = connRdmaSetReadHandler,
    .set_write_handler = connRdmaSetWriteHandler,
    .get_last_error = connRdmaGetLastError,
    .read = connRdmaRead,
    .write = connRdmaWrite,
    .close = connRdmaClose,
    // .connect = connRdmaConnect,
    // .blocking_connect = connRdmaBlockingConnect,
    // .sync_read = connRdmaSyncRead,
    // .sync_write = connRdmaSyncWrite,
    // .sync_readline = connRdmaSyncReadLine,
    .get_type = connRdmaGetType};

static int rdmaServer(char *err, int port, char *bindaddr, int af, int index)
{
    int s = ANET_OK, rv, afonly = 1;
    char _port[6]; /* strlen("65535") */
    struct addrinfo hints, *servinfo, *p;
    struct sockaddr_storage sock_addr;
    struct rdma_cm_id *listen_cmid;

    if (ibv_fork_init())
    {
        serverLog(LL_WARNING, "RDMA: FATAL error, recv corrupted cmd");
        return ANET_ERR;
    }

    snprintf(_port, 6, "%d", port);
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = af;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE; /* No effect if bindaddr != NULL */
    if (bindaddr && !strcmp("*", bindaddr))
        bindaddr = NULL;

    if (af == AF_INET6 && bindaddr && !strcmp("::*", bindaddr))
        bindaddr = NULL;

    if ((rv = getaddrinfo(bindaddr, _port, &hints, &servinfo)) != 0)
    {
        serverNetError(err, "RDMA: %s", gai_strerror(rv));
        return ANET_ERR;
    }
    else if (!servinfo)
    {
        serverNetError(err, "RDMA: get addr info failed");
        s = ANET_ERR;
        goto end;
    }

    for (p = servinfo; p != NULL; p = p->ai_next)
    {
        memset(&sock_addr, 0, sizeof(sock_addr));
        if (p->ai_family == AF_INET6)
        {
            memcpy(&sock_addr, p->ai_addr, sizeof(struct sockaddr_in6));
            ((struct sockaddr_in6 *)&sock_addr)->sin6_family = AF_INET6;
            ((struct sockaddr_in6 *)&sock_addr)->sin6_port = htons(port);
        }
        else
        {
            memcpy(&sock_addr, p->ai_addr, sizeof(struct sockaddr_in));
            ((struct sockaddr_in *)&sock_addr)->sin_family = AF_INET;
            ((struct sockaddr_in *)&sock_addr)->sin_port = htons(port);
        }

        if (rdma_create_id(listen_channel, &listen_cmid, NULL, RDMA_PS_TCP))
        {
            serverNetError(err, "RDMA: create listen cm id error");
            return ANET_ERR;
        }

        rdma_set_option(listen_cmid, RDMA_OPTION_ID, RDMA_OPTION_ID_AFONLY,
                        &afonly, sizeof(afonly));

        if (rdma_bind_addr(listen_cmid, (struct sockaddr *)&sock_addr))
        {
            serverNetError(err, "RDMA: bind addr error");
            goto error;
        }

        if (rdma_listen(listen_cmid, 0))
        {
            serverNetError(err, "RDMA: listen addr error");
            goto error;
        }

        listen_cmids[index] = listen_cmid;
        goto end;
    }

error:
    if (listen_cmid)
        rdma_destroy_id(listen_cmid);
    s = ANET_ERR;

end:
    freeaddrinfo(servinfo);
    return s;
}

// here the real server is created
int listenToRdma(int port, socketFds *sfd, const char* ib_devname)
{
    int j, index = 0, ret;
    char **bindaddr = server.bindaddr;
    int bindaddr_count = server.bindaddr_count;
    char *default_bindaddr[2] = {"*", "-::*"};

    memset(ib_devname_server, 0, sizeof(ib_devname_server));
    if (ib_devname)
        strncpy(ib_devname_server, ib_devname, sizeof(ib_devname_server) - 1);

    assert(server.proto_max_bulk_len <= 512ll * 1024 * 1024);

    /* Force binding of 0.0.0.0 if no bind address is specified. */
    if (server.bindaddr_count == 0)
    {
        bindaddr_count = 2;
        bindaddr = default_bindaddr;
    }

    listen_channel = rdma_create_event_channel();
    if (!listen_channel)
    {
        serverLog(LL_WARNING, "RDMA: Could not create event channel");
        return C_ERR;
    }

    for (j = 0; j < bindaddr_count; j++)
    {
        char *addr = bindaddr[j];
        int optional = *addr == '-';

        if (optional)
            addr++;
        if (strchr(addr, ':'))
        {
            /* Bind IPv6 address. */
            ret = rdmaServer(server.neterr, port, addr, AF_INET6, index);
        }
        else
        {
            /* Bind IPv4 address. */
            ret = rdmaServer(server.neterr, port, addr, AF_INET, index);
            // serverLog(LL_VERBOSE, "RDMA: server start to listen on %s:%d\n", addr, port);
        }

        if (ret == ANET_ERR)
        {
            int net_errno = errno;
            serverLog(LL_WARNING, "RDMA: Could not create server for %s:%d: %s",
                      addr, port, server.neterr);

            if (net_errno == EADDRNOTAVAIL && optional)
                continue;

            if (net_errno == ENOPROTOOPT || net_errno == EPROTONOSUPPORT ||
                net_errno == ESOCKTNOSUPPORT || net_errno == EPFNOSUPPORT ||
                net_errno == EAFNOSUPPORT)
                continue;

            return C_ERR;
        }

        index++;
    }

    // serverLog(LL_VERBOSE, "RDMA: server start to listen on %d ports\n", sfd->count);
    sfd->fd[sfd->count] = listen_channel->fd;
    // todo
    // int flags = fcntl(listen_channel->fd, F_GETFL, 0);
    // fcntl(listen_channel->fd, F_SETFL, flags | O_NONBLOCK);
    anetNonBlock(NULL, sfd->fd[sfd->count]);
    anetCloexec(sfd->fd[sfd->count]);
    sfd->count++;

    return C_OK;
}

// proceed to establish connection
static int rdmaHandleConnect(char *err, struct rdma_cm_event *ev, char *ip, size_t ip_len, int *port)
{
    int ret = C_OK;
    struct rdma_cm_id *cm_id = ev->id;
    struct sockaddr_storage caddr;
    RdmaContext *ctx = NULL;
    struct rdma_conn_param conn_param = {
        .responder_resources = 0,
        .initiator_depth = 0,
    };

    memcpy(&caddr, &cm_id->route.addr.dst_addr, sizeof(caddr));
    if (caddr.ss_family == AF_INET)
    {
        struct sockaddr_in *s = (struct sockaddr_in *)&caddr;
        if (ip)
            inet_ntop(AF_INET, (void *)&(s->sin_addr), ip, ip_len);
        if (port)
            *port = ntohs(s->sin_port);
    }
    else
    {
        struct sockaddr_in6 *s = (struct sockaddr_in6 *)&caddr;
        if (ip)
            inet_ntop(AF_INET6, (void *)&(s->sin6_addr), ip, ip_len);
        if (port)
            *port = ntohs(s->sin6_port);
    }

    ctx = zcalloc(sizeof(RdmaContext));
    ctx->timeEvent = -1;
    ctx->ip = zstrdup(ip);
    ctx->port = *port;
    cm_id->context = ctx;
    if (rdmaCreateResource(ctx, cm_id) == C_ERR)
    {
        goto reject;
    }

    conn_param.qp_num = cm_id->qp->qp_num;
    ret = rdma_accept(cm_id, &conn_param);
    if (ret)
    {
        serverNetError(err, "RDMA: accept failed");
        goto free_rdma;
    }

    for (int i = 0; i < REDIS_MAX_SGE; i++)
    {
        // size_t data_len = i % 2 ? REDIS_RDMA_SERVER_RX_SET_SIZE : REDIS_RDMA_SERVER_RX_GET_SIZE;
        if (rdmaPostRecv(ctx, cm_id, i, REDIS_RDMA_SERVER_RX_SIZE) == C_ERR)
        {
            serverLog(LL_WARNING, "RDMA: post recv failed");
            // goto destroy_iobuf;
        }
    }

    serverLog(LL_VERBOSE, "RDMA: successful connection from %s:%d\n", ctx->ip, ctx->port);
    return C_OK;

free_rdma:
    rdmaReleaseResource(ctx);
reject:
    /* reject connect request if hitting error */
    rdma_reject(cm_id, NULL, 0);

    return C_ERR;
}

/*
 * rdmaAccept, actually it works as cm-event handler for listen cm_id.
 * accept a connection logic works in two steps:
 * 1, handle RDMA_CM_EVENT_CONNECT_REQUEST and return CM fd on success
 * 2, handle RDMA_CM_EVENT_ESTABLISHED and return C_OK on success
 */
int rdmaAccept(char *err, int s, char *ip, size_t ip_len, int *port, void **priv)
{
    struct rdma_cm_event *ev;
    enum rdma_cm_event_type ev_type;
    int ret = C_OK;
    UNUSED(s);

    ret = rdma_get_cm_event(listen_channel, &ev);
    if (ret)
    {
        if (errno != EAGAIN)
        {
            serverLog(LL_WARNING, "RDMA: listen channel rdma_get_cm_event failed, %s", strerror(errno));
            return ANET_ERR;
        }
        return ANET_OK;
    }

    ev_type = ev->event;
    serverLog(LL_DEBUG, "RDMA: event type %s", rdma_event_str(ev_type));
    switch (ev_type)
    {
    case RDMA_CM_EVENT_CONNECT_REQUEST:
        ret = rdmaHandleConnect(err, ev, ip, ip_len, port);
        if (ret == C_OK)
        {
            // RdmaContext *ctx = (RdmaContext *)ev->id->context;
            *priv = ev->id;
            // todo: magic number as a placeholder for file descriptor
            ret = 0xff;
        }
        break;

    case RDMA_CM_EVENT_ESTABLISHED:
        ret = rdmaHandleEstablished(ev);
        break;

    case RDMA_CM_EVENT_UNREACHABLE:
    case RDMA_CM_EVENT_ADDR_ERROR:
    case RDMA_CM_EVENT_ROUTE_ERROR:
    case RDMA_CM_EVENT_CONNECT_ERROR:
    case RDMA_CM_EVENT_REJECTED:
    case RDMA_CM_EVENT_ADDR_CHANGE:
    case RDMA_CM_EVENT_DISCONNECTED:
    case RDMA_CM_EVENT_TIMEWAIT_EXIT:
        rdmaHandleDisconnect(ev);
        ret = C_OK;
        break;

    case RDMA_CM_EVENT_MULTICAST_JOIN:
    case RDMA_CM_EVENT_MULTICAST_ERROR:
    case RDMA_CM_EVENT_DEVICE_REMOVAL:
    case RDMA_CM_EVENT_ADDR_RESOLVED:
    case RDMA_CM_EVENT_ROUTE_RESOLVED:
    case RDMA_CM_EVENT_CONNECT_RESPONSE:
    default:
        serverLog(LL_NOTICE, "RDMA: listen channel ignore event: %s", rdma_event_str(ev_type));
        break;
    }

    if (rdma_ack_cm_event(ev))
    {
        serverLog(LL_WARNING, "ack cm event failed\n");
        return ANET_ERR;
    }

    return ret;
}

connection *connCreateRdma()
{
    rdma_connection *rdma_conn = zcalloc(sizeof(rdma_connection));
    rdma_conn->c.type = &CT_RDMA;
    rdma_conn->c.fd = -1;

    return (connection *)rdma_conn;
}

// note: connection is established, add to server
connection *connCreateAcceptedRdma(int fd, void *priv)
{
    rdma_connection *rdma_conn = (rdma_connection *)connCreateRdma();
    // todo: will not use this one
    rdma_conn->c.fd = fd;
    rdma_conn->c.state = CONN_STATE_ACCEPTING;
    rdma_conn->cm_id = priv;

    return (connection *)rdma_conn;
}
#else /* __linux__ */

"BUILD ERROR: RDMA is only supported on linux"

#endif /* __linux__ */
#else  /* USE_RDMA */
int listenToRdma(int port, socketFds *sfd)
{
    UNUSED(port);
    UNUSED(sfd);
    serverNetError(server.neterr, "RDMA: disabled, need rebuild with BUILD_RDMA=yes");

    return C_ERR;
}

int rdmaAccept(char *err, int s, char *ip, size_t ip_len, int *port, void **priv)
{
    UNUSED(err);
    UNUSED(s);
    UNUSED(ip);
    UNUSED(ip_len);
    UNUSED(port);
    UNUSED(priv);

    serverNetError(server.neterr, "RDMA: disabled, need rebuild with BUILD_RDMA=yes");
    errno = EOPNOTSUPP;

    return C_ERR;
}

connection *connCreateRdma()
{
    serverNetError(server.neterr, "RDMA: disabled, need rebuild with BUILD_RDMA=yes");
    errno = EOPNOTSUPP;

    return NULL;
}

connection *connCreateAcceptedRdma(int fd, void *priv)
{
    UNUSED(fd);
    UNUSED(priv);
    serverNetError(server.neterr, "RDMA: disabled, need rebuild with BUILD_RDMA=yes");
    errno = EOPNOTSUPP;

    return NULL;
}

#endif
