#pragma once
#include "duckdb.hpp"
#include <erl_nif.h>
#include <string>

namespace nif {
  bool get_config_name(ErlNifEnv* env, ERL_NIF_TERM term, std::string& sink);
  bool term_to_option_value(ErlNifEnv* env, ERL_NIF_TERM term, const duckdb::ConfigurationOption& option, duckdb::Value& sink);
  bool term_to_option_value(ErlNifEnv* env, ERL_NIF_TERM term, const duckdb::ExtensionOption& option, duckdb::Value& sink);
  ERL_NIF_TERM config_option_to_term(ErlNifEnv* env, const duckdb::ConfigurationOption& option);
}
