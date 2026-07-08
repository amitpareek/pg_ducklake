#include "pgddb/worker/worker_session.hpp"

#include <exception>
#include <string>

#include "pgddb/worker/transport/session_channel.hpp"
#include "pgddb/worker/transport/session_protocol.hpp"

#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/query_result.hpp>

namespace pgddb {

namespace {
void
SendError(SessionChannel &channel, const std::string &msg) {
	channel.SerializedSendResult(FrameTag::Error, msg.c_str(), msg.size());
}
} // namespace

void
RunWorkerSession(duckdb::Connection &con, SessionChannel &channel, const std::string &sql) {
	try {
		auto result = con.SendQuery(sql);
		if (result->HasError()) {
			SendError(channel, result->GetError());
			return;
		}
		while (true) {
			if (channel.IsCancelRequested()) {
				con.Interrupt();
				SendError(channel, "query cancelled");
				return;
			}
			auto chunk = result->Fetch();
			if (!chunk || chunk->size() == 0)
				break;
			duckdb::MemoryStream stream;
			SerializeDataChunk(*chunk, stream);
			if (!channel.SerializedSendResult(FrameTag::Chunk, (const char *)stream.GetData(), stream.GetPosition()))
				return; /* backend detached */
		}
		channel.SerializedSendResult(FrameTag::Complete, nullptr, 0);
	} catch (const std::exception &e) {
		SendError(channel, e.what());
	}
}

} // namespace pgddb
