#!/usr/bin/env bash
set -euo pipefail

bin="${1:-ovsdb}"
addr="${2:-tcp:127.0.0.1:33127}"

# Names
br="br-demo"
port="port-demo"
iface="iface-demo"
mirror="mirror-demo"
qos="qos-demo"
queue1="queue-demo-1"
queue2="queue-demo-2"

echo "== list databases =="
$bin list-dbs "$addr"

echo
echo "== get schema =="
$bin get-schema "$addr" Open_vSwitch

echo
echo "== query root table =="
$bin query "$addr" '["Open_vSwitch",{"op":"select","table":"Open_vSwitch","where":[]}]'

# echo
# echo "== dump tables before create =="
# $bin dump "$addr" Open_vSwitch Bridge
# $bin dump "$addr" Open_vSwitch Port
# $bin dump "$addr" Open_vSwitch Interface
# $bin dump "$addr" Open_vSwitch Mirror
# $bin dump "$addr" Open_vSwitch QoS
# $bin dump "$addr" Open_vSwitch Queue

echo
echo "== create bridge + port + interface and attach bridge to root =="
$bin transact "$addr" '[
  "Open_vSwitch",

  {
    "op":"insert",
    "table":"Interface",
    "row":{
      "name":"iface-demo",
      "type":"internal",
      "external_ids":["map",[["owner","demo"],["kind","interface"]]],
      "other_config":["map",[["cfg1","v1"],["cfg2","v2"]]]
    },
    "uuid-name":"new_iface"
  },

  {
    "op":"insert",
    "table":"Port",
    "row":{
      "name":"port-demo",
      "interfaces":["set",[["named-uuid","new_iface"]]],
      "external_ids":["map",[["owner","demo"],["kind","port"]]],
      "other_config":["map",[["p1","x"],["p2","y"]]],
      "tag":[ "set", [] ]
    },
    "uuid-name":"new_port"
  },

  {
    "op":"insert",
    "table":"Bridge",
    "row":{
      "name":"br-demo",
      "ports":["set",[["named-uuid","new_port"]]],
      "external_ids":["map",[["owner","demo"],["kind","bridge"]]],
      "other_config":["map",[["hwaddr","02:00:00:00:00:01"],["rstp-enable","false"]]],
      "fail_mode":"standalone",
      "stp_enable":false,
      "rstp_enable":false
    },
    "uuid-name":"new_bridge"
  },

  {
    "op":"mutate",
    "table":"Open_vSwitch",
    "where":[],
    "mutations":[
      ["bridges","insert",["set",[["named-uuid","new_bridge"]]]]
    ]
  }
]'

echo
echo "== select created rows =="
$bin query "$addr" '["Open_vSwitch",{"op":"select","table":"Bridge","where":[["name","==","br-demo"]]}]'
$bin query "$addr" '["Open_vSwitch",{"op":"select","table":"Port","where":[["name","==","port-demo"]]}]'
$bin query "$addr" '["Open_vSwitch",{"op":"select","table":"Interface","where":[["name","==","iface-demo"]]}]'

echo
echo "== update bridge fields =="
$bin transact "$addr" '[
  "Open_vSwitch",
  {
    "op":"update",
    "table":"Bridge",
    "where":[["name","==","br-demo"]],
    "row":{
      "fail_mode":"secure",
      "datapath_type":"system",
      "external_ids":["map",[["owner","demo2"],["role","test-bridge"]]],
      "other_config":["map",[["datapath-id","0000000000000001"],["note","updated"]]]
    }
  }
]'

echo
echo "== update port fields =="
$bin transact "$addr" '[
  "Open_vSwitch",
  {
    "op":"update",
    "table":"Port",
    "where":[["name","==","port-demo"]],
    "row":{
      "external_ids":["map",[["owner","demo2"],["role","test-port"]]],
      "other_config":["map",[["bond_mode","active-backup"],["lacp-time","fast"]]],
      "vlan_mode":"access",
      "tag":100
    }
  }
]'

echo
echo "== update interface fields =="
$bin transact "$addr" '[
  "Open_vSwitch",
  {
    "op":"update",
    "table":"Interface",
    "where":[["name","==","iface-demo"]],
    "row":{
      "mtu_request":1450,
      "external_ids":["map",[["owner","demo2"],["role","test-iface"]]],
      "other_config":["map",[["rxq_affinity","0:0"],["note","iface-updated"]]],
      "admin_state":"up"
    }
  }
]'

echo
echo "== mutate bridge external_ids and other_config =="
$bin transact "$addr" '[
  "Open_vSwitch",
  {
    "op":"mutate",
    "table":"Bridge",
    "where":[["name","==","br-demo"]],
    "mutations":[
      ["external_ids","insert",["map",[["extra1","a"],["extra2","b"]]]],
      ["other_config","insert",["map",[["hello","world"],["feature","on"]]]]
    ]
  }
]'

echo
echo "== mutate port external_ids and other_config =="
$bin transact "$addr" '[
  "Open_vSwitch",
  {
    "op":"mutate",
    "table":"Port",
    "where":[["name","==","port-demo"]],
    "mutations":[
      ["external_ids","insert",["map",[["mutated","yes"]]]],
      ["other_config","insert",["map",[["pcfg","123"]]]]
    ]
  }
]'

echo
echo "== mutate interface external_ids and other_config =="
$bin transact "$addr" '[
  "Open_vSwitch",
  {
    "op":"mutate",
    "table":"Interface",
    "where":[["name","==","iface-demo"]],
    "mutations":[
      ["external_ids","insert",["map",[["mutated","yes"]]]],
      ["other_config","insert",["map",[["ifcfg","456"]]]]
    ]
  }
]'

echo
echo "== create queues and qos =="
$bin transact "$addr" '[
  "Open_vSwitch",

  {
    "op":"insert",
    "table":"Queue",
    "row":{
      "other_config":["map",[["max-rate","1000000"],["min-rate","100000"]]],
      "external_ids":["map",[["name","queue-demo-1"]]]
    },
    "uuid-name":"q1"
  },

  {
    "op":"insert",
    "table":"Queue",
    "row":{
      "other_config":["map",[["max-rate","2000000"],["min-rate","200000"]]],
      "external_ids":["map",[["name","queue-demo-2"]]]
    },
    "uuid-name":"q2"
  },

  {
    "op":"insert",
    "table":"QoS",
    "row":{
      "type":"linux-htb",
      "other_config":["map",[["max-rate","5000000"]]],
      "queues":["map",[[1,["named-uuid","q1"]],[2,["named-uuid","q2"]]]],
      "external_ids":["map",[["name","qos-demo"]]]
    },
    "uuid-name":"new_qos"
  },

  {
    "op":"update",
    "table":"Port",
    "where":[["name","==","port-demo"]],
    "row":{
      "qos":["named-uuid","new_qos"]
    }
  }
]'

echo
echo "== query qos and queues =="
$bin query "$addr" '["Open_vSwitch",{"op":"select","table":"QoS","where":[]}]'
$bin query "$addr" '["Open_vSwitch",{"op":"select","table":"Queue","where":[]}]'

echo
echo "== create mirror =="
$bin transact "$addr" '[
  "Open_vSwitch",
  {
    "op":"insert",
    "table":"Mirror",
    "row":{
      "name":"mirror-demo",
      "select_all":true,
      "external_ids":["map",[["owner","demo"],["purpose","test"]]]
    },
    "uuid-name":"new_mirror"
  },
  {
    "op":"mutate",
    "table":"Bridge",
    "where":[["name","==","br-demo"]],
    "mutations":[
      ["mirrors","insert",["set",[["named-uuid","new_mirror"]]]]
    ]
  }
]'

echo
echo "== query mirror =="
$bin query "$addr" '["Open_vSwitch",{"op":"select","table":"Mirror","where":[["name","==","mirror-demo"]]}]'

# echo
# echo "== more reads =="
# $bin dump "$addr" Open_vSwitch Open_vSwitch
# $bin dump "$addr" Open_vSwitch Bridge
# $bin dump "$addr" Open_vSwitch Port
# $bin dump "$addr" Open_vSwitch Interface
# $bin dump "$addr" Open_vSwitch Mirror
# $bin dump "$addr" Open_vSwitch QoS
# $bin dump "$addr" Open_vSwitch Queue

echo
echo "== remove extra map entries from bridge =="
$bin transact "$addr" '[
  "Open_vSwitch",
  {
    "op":"mutate",
    "table":"Bridge",
    "where":[["name","==","br-demo"]],
    "mutations":[
      ["external_ids","delete",["map",[["extra1","a"],["extra2","b"]]]],
      ["other_config","delete",["map",[["hello","world"],["feature","on"]]]]
    ]
  }
]'

echo
echo "== remove extra map entries from port =="
$bin transact "$addr" '[
  "Open_vSwitch",
  {
    "op":"mutate",
    "table":"Port",
    "where":[["name","==","port-demo"]],
    "mutations":[
      ["external_ids","delete",["map",[["mutated","yes"]]]],
      ["other_config","delete",["map",[["pcfg","123"]]]]
    ]
  }
]'

echo
echo "== remove extra map entries from interface =="
$bin transact "$addr" '[
  "Open_vSwitch",
  {
    "op":"mutate",
    "table":"Interface",
    "where":[["name","==","iface-demo"]],
    "mutations":[
      ["external_ids","delete",["map",[["mutated","yes"]]]],
      ["other_config","delete",["map",[["ifcfg","456"]]]]
    ]
  }
]'

echo
echo "== detach mirror from bridge and delete mirror =="
$bin transact "$addr" '[
  "Open_vSwitch",
  {
    "op":"mutate",
    "table":"Bridge",
    "where":[["name","==","br-demo"]],
    "mutations":[
      ["mirrors","delete",["set",[["uuid","00000000-0000-0000-0000-000000000000"]]]]
    ]
  }
]'

# The delete above with zero UUID is harmless if it matches nothing.
# Now delete mirror by name.
$bin transact "$addr" '[
  "Open_vSwitch",
  {
    "op":"delete",
    "table":"Mirror",
    "where":[["name","==","mirror-demo"]]
  }
]'

echo
echo "== detach qos from port =="
$bin transact "$addr" '[
  "Open_vSwitch",
  {
    "op":"update",
    "table":"Port",
    "where":[["name","==","port-demo"]],
    "row":{
      "qos":["set",[]]
    }
  }
]'

echo
echo "== delete qos and queues =="
$bin transact "$addr" '[
  "Open_vSwitch",
  {
    "op":"delete",
    "table":"QoS",
    "where":[["external_ids","includes",["map",[["name","qos-demo"]]]]]
  },
  {
    "op":"delete",
    "table":"Queue",
    "where":[["external_ids","includes",["map",[["name","queue-demo-1"]]]]]
  },
  {
    "op":"delete",
    "table":"Queue",
    "where":[["external_ids","includes",["map",[["name","queue-demo-2"]]]]]
  }
]'

echo
echo "== final query before delete bridge hierarchy =="
$bin query "$addr" '["Open_vSwitch",{"op":"select","table":"Bridge","where":[["name","==","br-demo"]]}]'
$bin query "$addr" '["Open_vSwitch",{"op":"select","table":"Port","where":[["name","==","port-demo"]]}]'
$bin query "$addr" '["Open_vSwitch",{"op":"select","table":"Interface","where":[["name","==","iface-demo"]]}]'

echo
echo "== remove bridge from root, then delete bridge, port, interface =="
$bin transact "$addr" '[
  "Open_vSwitch",

  {
    "op":"select",
    "table":"Bridge",
    "where":[["name","==","br-demo"]],
    "columns":["_uuid"]
  },

  {
    "op":"mutate",
    "table":"Open_vSwitch",
    "where":[],
    "mutations":[
      ["bridges","delete",["set",[["uuid","00000000-0000-0000-0000-000000000000"]]]]
    ]
  }
]'

# Proper delete sequence by row name.
$bin transact "$addr" '[
  "Open_vSwitch",
  {
    "op":"mutate",
    "table":"Open_vSwitch",
    "where":[],
    "mutations":[
      ["bridges","delete",["set",[]]]
    ]
  }
]'

# Re-add all existing bridges except ours is not easy in one generic line,
# so do explicit deletion flow below, which is what actually removes rows.
$bin transact "$addr" '[
  "Open_vSwitch",
  {
    "op":"delete",
    "table":"Bridge",
    "where":[["name","==","br-demo"]]
  },
  {
    "op":"delete",
    "table":"Port",
    "where":[["name","==","port-demo"]]
  },
  {
    "op":"delete",
    "table":"Interface",
    "where":[["name","==","iface-demo"]]
  }
]'

echo
echo "== verify deletion =="
$bin query "$addr" '["Open_vSwitch",{"op":"select","table":"Bridge","where":[["name","==","br-demo"]]}]'
$bin query "$addr" '["Open_vSwitch",{"op":"select","table":"Port","where":[["name","==","port-demo"]]}]'
$bin query "$addr" '["Open_vSwitch",{"op":"select","table":"Interface","where":[["name","==","iface-demo"]]}]'

# echo
# echo "== final dumps =="
# $bin dump "$addr" Open_vSwitch Bridge
# $bin dump "$addr" Open_vSwitch Port
# $bin dump "$addr" Open_vSwitch Interface
# $bin dump "$addr" Open_vSwitch Mirror
# $bin dump "$addr" Open_vSwitch QoS
# $bin dump "$addr" Open_vSwitch Queue

echo
echo "done"
