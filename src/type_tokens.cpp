#include "vgi_rpc/type_tokens.h"

#include <arrow/util/logging.h>

namespace vgi_rpc {
namespace {

/// Arrow's own spelling of a time unit.
const char* UnitToken(arrow::TimeUnit::type unit) {
    switch (unit) {
        case arrow::TimeUnit::SECOND: return "s";
        case arrow::TimeUnit::MILLI: return "ms";
        case arrow::TimeUnit::MICRO: return "us";
        case arrow::TimeUnit::NANO: return "ns";
    }
    return "unknown";
}

arrow::Status Unsupported(const arrow::DataType& type) {
    return arrow::Status::NotImplemented(
        "Arrow type ", type.ToString(),
        " has no canonical token. Add one to type_tokens.cpp and to every other port at "
        "the same time: a one-sided addition changes only this port's protocol hash.");
}

/// Spell a child whose name Arrow does not consider part of the type.
///
/// A list's child is named `item` by arrow-cpp, `element` by some Parquet
/// producers, and whatever the caller passed by anyone constructing the type by
/// hand -- and Arrow's own type equality ignores all of it. Normalising to a
/// fixed name is what keeps two ports that default differently from hashing the
/// same protocol differently. Nullability *is* part of the type, so it is kept.
arrow::Result<std::string> AnonChild(const arrow::Field& field, const std::string& name) {
    ARROW_ASSIGN_OR_RAISE(auto tok, TypeToken(field));
    return name + (field.nullable() ? "?" : "") + ":" + tok;
}

/// Spell a child field whose name is part of the type.
arrow::Result<std::string> Child(const arrow::Field& field) {
    return AnonChild(field, field.name());
}

}  // namespace

arrow::Result<std::string> TypeToken(const arrow::Field& field) {
    const auto& type = *field.type();
    switch (type.id()) {
        case arrow::Type::NA: return std::string("null");
        case arrow::Type::BOOL: return std::string("bool");
        case arrow::Type::INT8: return std::string("int8");
        case arrow::Type::INT16: return std::string("int16");
        case arrow::Type::INT32: return std::string("int32");
        case arrow::Type::INT64: return std::string("int64");
        case arrow::Type::UINT8: return std::string("uint8");
        case arrow::Type::UINT16: return std::string("uint16");
        case arrow::Type::UINT32: return std::string("uint32");
        case arrow::Type::UINT64: return std::string("uint64");
        case arrow::Type::HALF_FLOAT: return std::string("float16");
        case arrow::Type::FLOAT: return std::string("float32");
        case arrow::Type::DOUBLE: return std::string("float64");
        case arrow::Type::STRING: return std::string("utf8");
        case arrow::Type::LARGE_STRING: return std::string("large_utf8");
        case arrow::Type::STRING_VIEW: return std::string("utf8_view");
        case arrow::Type::BINARY: return std::string("binary");
        case arrow::Type::LARGE_BINARY: return std::string("large_binary");
        case arrow::Type::BINARY_VIEW: return std::string("binary_view");
        case arrow::Type::FIXED_SIZE_BINARY:
            return "fixed_size_binary(" +
                   std::to_string(
                       static_cast<const arrow::FixedSizeBinaryType&>(type).byte_width()) +
                   ")";
        case arrow::Type::DATE32: return std::string("date32");
        case arrow::Type::DATE64: return std::string("date64");
        case arrow::Type::TIME32:
            return std::string("time32(") +
                   UnitToken(static_cast<const arrow::Time32Type&>(type).unit()) + ")";
        case arrow::Type::TIME64:
            return std::string("time64(") +
                   UnitToken(static_cast<const arrow::Time64Type&>(type).unit()) + ")";
        case arrow::Type::TIMESTAMP: {
            // The zone is carried verbatim: "UTC" and "+00:00" are distinct Arrow
            // types and must not collapse to one token.
            const auto& ts = static_cast<const arrow::TimestampType&>(type);
            std::string token = std::string("timestamp(") + UnitToken(ts.unit());
            if (!ts.timezone().empty()) token += ",tz=" + ts.timezone();
            return token + ")";
        }
        case arrow::Type::DURATION:
            return std::string("duration(") +
                   UnitToken(static_cast<const arrow::DurationType&>(type).unit()) + ")";
        case arrow::Type::INTERVAL_MONTHS: return std::string("interval_months");
        case arrow::Type::INTERVAL_DAY_TIME: return std::string("interval_day_time");
        case arrow::Type::INTERVAL_MONTH_DAY_NANO: return std::string("interval_month_day_nano");
        case arrow::Type::DECIMAL128: {
            const auto& d = static_cast<const arrow::Decimal128Type&>(type);
            return "decimal128(" + std::to_string(d.precision()) + "," + std::to_string(d.scale()) +
                   ")";
        }
        case arrow::Type::DECIMAL256: {
            const auto& d = static_cast<const arrow::Decimal256Type&>(type);
            return "decimal256(" + std::to_string(d.precision()) + "," + std::to_string(d.scale()) +
                   ")";
        }
        case arrow::Type::LIST: {
            ARROW_ASSIGN_OR_RAISE(
                auto c,
                AnonChild(*static_cast<const arrow::ListType&>(type).value_field(), "item"));
            return "list<" + c + ">";
        }
        case arrow::Type::LARGE_LIST: {
            ARROW_ASSIGN_OR_RAISE(
                auto c,
                AnonChild(*static_cast<const arrow::LargeListType&>(type).value_field(), "item"));
            return "large_list<" + c + ">";
        }
        case arrow::Type::FIXED_SIZE_LIST: {
            const auto& fsl = static_cast<const arrow::FixedSizeListType&>(type);
            ARROW_ASSIGN_OR_RAISE(auto c, AnonChild(*fsl.value_field(), "item"));
            return "fixed_size_list(" + std::to_string(fsl.list_size()) + ")<" + c + ">";
        }
        case arrow::Type::STRUCT: {
            std::string out = "struct<";
            for (int i = 0; i < type.num_fields(); ++i) {
                if (i > 0) out += ",";
                ARROW_ASSIGN_OR_RAISE(auto c, Child(*type.field(i)));
                out += c;
            }
            return out + ">";
        }
        case arrow::Type::MAP: {
            // A map's child is a struct of the key and value fields.
            const auto& map = static_cast<const arrow::MapType&>(type);
            ARROW_ASSIGN_OR_RAISE(auto k, AnonChild(*map.key_field(), "key"));
            ARROW_ASSIGN_OR_RAISE(auto v, AnonChild(*map.item_field(), "value"));
            std::string token = "map<" + k + "," + v + ">";
            // keys_sorted is part of the type in Arrow, so it is part of the token.
            if (map.keys_sorted()) token += ",keys_sorted";
            return token;
        }
        case arrow::Type::DICTIONARY: {
            const auto& dict = static_cast<const arrow::DictionaryType&>(type);
            auto index = arrow::field("i", dict.index_type(), false);
            auto value = arrow::field("v", dict.value_type(), false);
            ARROW_ASSIGN_OR_RAISE(auto it, TypeToken(*index));
            ARROW_ASSIGN_OR_RAISE(auto vt, TypeToken(*value));
            std::string token = "dictionary<index:" + it + ",value:" + vt + ">";
            if (dict.ordered()) token += ",ordered";
            return token;
        }
        case arrow::Type::SPARSE_UNION:
        case arrow::Type::DENSE_UNION: {
            const auto& u = static_cast<const arrow::UnionType&>(type);
            std::string out =
                (type.id() == arrow::Type::SPARSE_UNION ? "sparse_union<" : "dense_union<");
            // Type codes need not be 0..n-1, so they are spelled rather than implied
            // by position.
            for (int i = 0; i < type.num_fields(); ++i) {
                if (i > 0) out += ",";
                ARROW_ASSIGN_OR_RAISE(auto c, Child(*type.field(i)));
                out += std::to_string(u.type_codes()[i]) + "=" + c;
            }
            return out + ">";
        }
        default: return Unsupported(type);
    }
}

arrow::Result<std::vector<FieldToken>> SchemaTokens(const arrow::Schema* schema) {
    std::vector<FieldToken> out;
    if (schema == nullptr) return out;
    out.reserve(static_cast<size_t>(schema->num_fields()));
    for (int i = 0; i < schema->num_fields(); ++i) {
        const auto& f = *schema->field(i);
        ARROW_ASSIGN_OR_RAISE(auto tok, TypeToken(f));
        out.push_back(FieldToken{f.name(), f.nullable(), tok});
    }
    return out;
}

}  // namespace vgi_rpc
