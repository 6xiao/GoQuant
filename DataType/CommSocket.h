#ifndef COMM_SOCKET_H
#define COMM_SOCKET_H

#include <windows.h>
#include <stdio.h>
#include <malloc.h>
#include <stdint.h>
#include <iostream>
#include <vector>
#include <time.h>
#include "DataType.h"
#include "DataType.pb.h"

using namespace DataType;

#pragma comment(lib,"ws2_32.lib")

#define C(MsgT,msg) do {if(E_##MsgT==msg->PbHead.pbType){MsgT tmp; tmp.ParseFromArray(msg->Buf, msg->PbHead.pbLength);OnRecv##MsgT(&msg->MsgHead, &tmp); }} while (0)
#define F(MsgT) virtual void OnRecv##MsgT(MessageHead * head, MsgT * msg){};\
	void Send##MsgT(MessageHead * head, MsgT * msg){\
	if (!head || !msg) return;\
	int len = msg->ByteSize();\
	if (len < 512 * 1024){\
	MsgWithBuf_512KB tmp;\
	tmp.MsgHead = *head;\
	tmp.PbHead.pbType = E_##MsgT;\
	tmp.PbHead.pbLength = len;\
	msg->SerializeToArray(tmp.Buf, len);\
	tmp.PbHead.HashCode = Hash(tmp.Buf, len);\
	Send(&tmp, HEAD_LEN() + len);\
	}else if (len < 512 * 1024 * 1024) {\
	MsgWithBuf_512M * tmp = new(MsgWithBuf_512M);\
	tmp->MsgHead = *head;\
	tmp->PbHead.pbType = E_##MsgT;\
	tmp->PbHead.pbLength = len;\
	msg->SerializeToArray(tmp->Buf, len);\
	tmp->PbHead.HashCode = Hash(tmp->Buf, len);\
	Send(tmp, HEAD_LEN() + len);\
	delete tmp;\
	}else{OutputDebugStringA("length > 512MB\r\n");} };

using namespace DataType;

class CommSocket
{
private :
	CommSocket();
	CommSocket(const CommSocket&);
	CommSocket & operator=(const CommSocket&);

	SOCKET socket_client;
	struct sockaddr_in svr_address;
	MsgWithBuf_512M * msg;

protected :
	CommSocket(const char * ip, uint16_t port)
	{
		WSADATA wsa;
		WSAStartup(MAKEWORD(2, 2), &wsa);

		memset(&svr_address, 0, sizeof(svr_address));
		svr_address.sin_family = AF_INET;
		svr_address.sin_port = htons(port);
		svr_address.sin_addr.S_un.S_addr = inet_addr(ip);

		socket_client = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);

		msg = new(MsgWithBuf_512M);
		if (!msg)
		{
			exit(-3);
		}
	}

public :
	~CommSocket()
	{
		closesocket(socket_client);
		WSACleanup();
		delete msg;
	}

	bool Send(void * mem, int64_t len)
	{
		if (sizeof(len) != send(socket_client, (char *)&len, sizeof(len), 0))
		{
			return false;
		}

		char * beg = (char *)mem;
		char * end = len + beg;

		while (beg < end)
		{
			int nw = send(socket_client, beg, end - beg, 0);
			if (nw <= 0)
			{
				return false;
			}
			beg += nw;
		}

		return beg == end;
	}

	uint64_t RegisterChannel(char * name)
	{
		MsgWithoutBuf msg;
		memset(&msg, 0, sizeof(msg));
		msg.MsgHead.SenderId = MakeID(name, strlen(name));
		msg.MsgHead.RecverId = msg.MsgHead.SenderId;
		Send(&msg, sizeof(msg));

		return msg.MsgHead.SenderId;
	};

	virtual void OnConnected(){}
	virtual void OnRecvIdle(){};
	virtual void OnRecvEmpty(MessageHead * head){};
	virtual void OnRecvError(char * error){};
	virtual void OnHashError(MessageHead * head, ProtoBufHead * pb, uint8_t * buf){};

	void Run()
	{
		if (INVALID_SOCKET != socket_client)
		{
			if (SOCKET_ERROR != connect(socket_client, (sockaddr *)&svr_address, sizeof(svr_address)))
			{
				OnConnected();

				fd_set fdread;

				for (;;)
				{
					FD_ZERO(&fdread);
					FD_SET(socket_client, &fdread);
					struct timeval tv = { 1, 0 };

					if (0 == select(socket_client + 1, &fdread, NULL, NULL, &tv))
					{
						//一秒未收到网络消息
						OnRecvIdle();
						continue;
					}

					memset(msg, 0, sizeof(MsgWithoutBuf));

					uint64_t nethead = 0;
					if (recv(socket_client, (char *)&nethead, sizeof(nethead), 0) != sizeof(nethead))
					{
						OnRecvError("Can't recv msg-head, may be disconnected");
						return;
					}

					char * reader = (char *)msg;
					int nr = 0;
					for (char *eos = reader + nethead; reader < eos; reader+=nr)
					{
						nr = recv(socket_client, reader, eos - reader, 0);

						if (nr < 0)
						{
							if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
							{
								continue;
							}
							else
							{
								OnRecvError("Can't recv msg-body fully, may be disconnected");
								return;
							}
						}
					}
					
					if (msg->PbHead.HashCode == 0 && msg->PbHead.pbLength == 0)
					{
						OnRecvEmpty(&msg->MsgHead);
					}
					else if (nethead != sizeof(msg->MsgHead) + sizeof(msg->PbHead) + msg->PbHead.pbLength)
					{
						OnRecvError("message trans-len != message body-len");
					}
					else if (msg->PbHead.HashCode != Hash(msg->Buf, msg->PbHead.pbLength))
					{
						OnHashError(&msg->MsgHead, &msg->PbHead, msg->Buf);
					}
					else
					{
						C(ReqSendOrder, msg);
						C(RspSendOrder, msg);
						C(ReqCancelOrder, msg);
						C(RspCancelOrder, msg);
						C(ReqOrdersInfo, msg);
						C(RspOrdersInfo, msg);
						C(ReqPosition, msg);
						C(RspPosition, msg);
						C(ReqAccount, msg);
						C(RspAccount, msg);
						C(ReqAlgoOrder, msg);
						C(RspAlgoOrder, msg);
						C(ReqTickerInfo, msg);
						C(RspTickerInfo, msg);
						C(DepthMarketData, msg);
						C(StaticMarketData, msg);
						C(ReqKLine, msg);
						C(RspKLine, msg);
						C(ReqTick, msg);
						C(RspTick, msg);
						C(HeartBeat, msg);
					}
				}
			}
		}
	}

	F(ReqSendOrder);
	F(RspSendOrder);
	F(ReqCancelOrder);
	F(RspCancelOrder);
	F(ReqOrdersInfo);
	F(RspOrdersInfo);
	F(ReqPosition);
	F(RspPosition);
	F(ReqAccount);
	F(RspAccount);
	F(ReqAlgoOrder);
	F(RspAlgoOrder);
	F(ReqTickerInfo);
	F(RspTickerInfo);
	F(DepthMarketData);
	F(StaticMarketData);
	F(ReqKLine);
	F(RspKLine);
	F(ReqTick);
	F(RspTick);
	F(HeartBeat);
};

#endif