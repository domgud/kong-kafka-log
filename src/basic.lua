local tablex = require "pl.tablex"
local resty_sha256 = require "resty.sha256"
local str = require "resty.string"
local _M = {}
local EMPTY = tablex.readonly({})
local gkong = kong
local gmatch = string.gmatch
local type = type
local ipairs = ipairs
local re_gmatch = ngx.re.gmatch
local tostring = tostring
local tonumber = tonumber
local ceil = math.ceil
local floor = math.floor
local socket = require("socket")
local uuid = require("uuid")
local json = require('cjson')
uuid.seed()

local function obfuscateJson(line)
  local t = json.decode(line) 
  local result = {}
  for k, v in pairs(t) do
      if k == "password"
      then
          result[k] = "<hidden>"
      else
          result[k] = v
   end
  end
  return result
end

local function obfuscatePayloadOrError()
  local requestBodyJson = ngx.req.get_body_data()
  if requestBodyJson == nil then
      return {["info"]="Payload empty"}
  end
  local status, returnValue = pcall(obfuscateJson, requestBodyJson)
      if status then
          return returnValue
      else
          return {["error"]="Error while deserializing body"}
      end
end

function _M.serialize(ngx, kong, conf)
  local ctx = ngx.ctx
  local var = ngx.var
  local req = ngx.req
  if not kong then
    kong = gkong
  end

  local PathOnly
  if var.request_uri ~= nil then
      PathOnly = string.gsub(var.request_uri,"%?.*","")
  end

  local UpstreamPathOnly
  if var.upstream_uri ~= nil then
      UpstreamPathOnly = string.gsub(var.upstream_uri,"%?.*","")
  end

  local BackendIp
  local BackendPort
  local DestHostName
  if ctx.balancer_data and ctx.balancer_data.tries then
      DestHostName = ctx.balancer_data.host
      if ctx.balancer_data.tries[1] then
        BackendIp = ctx.balancer_data.tries[1]["ip"]
        BackendPort = ctx.balancer_data.tries[1]["port"]
      end
  end

  local serviceName
  if ctx.service ~= nil then
        serviceName = ctx.service.name
  end


  local temp_request = "http://" .. var.host
  if PathOnly then
         temp_request = temp_request .. PathOnly
  end
  local Method = kong.request.get_method()
  local Payload
  if Method == "POST" or Method == "PUT" or Method == "PATCH" then
    Payload = obfuscatePayloadOrError()
  end
  return {

      payload = Payload,
      metadata = {
        name = serviceName,
        created_at = req.start_time() * 1000,
        id = uuid()
      },
      kong_host = {
          hostname = var.hostname,
          ip4 = var.server_addr
      },
      client_host = {
          ip4 = var.remote_addr
      },
      destination_host = {
          hostname = DestHostName,
          ipv4 = BackendIp,
          port = BackendPort,
          path = UpstreamPathOnly
      },
      request = {
          request = temp_request,
          method = Method,
          status = var.status,
          agent = var.http_user_agent,
          request_length = tonumber(var.request_length),
          bytes_sent = tonumber(var.bytes_sent)
      }
  }
end

return _M
