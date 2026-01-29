import os
import re
import json
import csv
import sys
import argparse
import time
from dataclasses import dataclass, asdict, field
from typing import Optional, Literal, Dict, Any

from telethon import TelegramClient, events
from telethon.tl.types import Message

# ---------- Config ----------
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

TG_API_ID = int(os.getenv("TG_API_ID", "0"))
TG_API_HASH = os.getenv("TG_API_HASH", "")
TG_SESSION = os.getenv("TG_SESSION", "therealfx_user")
TG_GROUP = os.getenv("TG_GROUP", "")

SYMBOL_POSTFIX = os.getenv("SYMBOL_POSTFIX", "-STD")
OUT_DIR = os.getenv("OUT_DIR", "out")

CSV_PATH = os.path.join(OUT_DIR, "copytrade_commands.csv")
JSONL_PATH = os.path.join(OUT_DIR, "copytrade_commands.jsonl")
STATE_PATH = os.path.join(OUT_DIR, "state.json")
DB_PATH = os.path.join(OUT_DIR, "message_order_db.json")

os.makedirs(OUT_DIR, exist_ok=True)

# ---------- Models ----------
Action = Literal["OPEN", "UPDATE", "CANCEL"]
OrderType = Literal["MARKET", "BUY_LIMIT", "SELL_LIMIT", "BUY_STOP", "SELL_STOP"]

@dataclass
class Command:
    cmd_id: str                    # e.g. "tg_12345"
    action: Action                 # OPEN / UPDATE / CANCEL
    symbol: str                    # e.g. "CADCHF-STD"
    type: OrderType                # MARKET / BUY_LIMIT / SELL_LIMIT ...
    side: Literal["BUY", "SELL"]    # BUY / SELL
    entry: float = 0.0             # for pending; for market = 0
    sl: float = 0.0
    tp: float = 0.0
    old_entry: float = 0.0         # used for UPDATE and sometimes CANCEL matching
    order_id: str = ""             # linked order ID (e.g., "order_123")
    meta: Optional[Dict[str, Any]] = None


# ---------- State (dedupe) ----------
def load_state() -> Dict[str, Any]:
    if not os.path.exists(STATE_PATH):
        return {"processed_ids": {}}
    with open(STATE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def save_state(state: Dict[str, Any]) -> None:
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp, STATE_PATH)

STATE = load_state()

def already_processed(msg_id: int) -> bool:
    return str(msg_id) in STATE.get("processed_ids", {})

def mark_processed(msg_id: int, ts: int) -> None:
    STATE.setdefault("processed_ids", {})[str(msg_id)] = ts
    # keep file from growing forever (optional: keep last 10k)
    if len(STATE["processed_ids"]) > 20000:
        # drop oldest ~2000
        items = sorted(STATE["processed_ids"].items(), key=lambda kv: kv[1])
        for k, _ in items[:2000]:
            del STATE["processed_ids"][k]
    save_state(STATE)


# ---------- Message-Order Database ----------
def load_db() -> Dict[str, Any]:
    if not os.path.exists(DB_PATH):
        return {"messages": {}, "orders": {}}
    try:
        with open(DB_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"messages": {}, "orders": {}}

def save_db(db: Dict[str, Any]) -> None:
    tmp = DB_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(db, f, ensure_ascii=False, indent=2)
    os.replace(tmp, DB_PATH)

DB = load_db()

def get_order_id_for_msg(msg_id: int) -> Optional[str]:
    """Retrieve order_id linked to a message ID."""
    msg_key = str(msg_id)
    if msg_key in DB["messages"]:
        return DB["messages"][msg_key].get("order_id")
    return None

def link_message_to_order(msg_id: int, order_id: str, cmd: Command) -> None:
    """Link a message to an order and store message data."""
    msg_key = str(msg_id)
    DB["messages"][msg_key] = {
        "order_id": order_id,
        "msg_id": msg_id,
        "action": cmd.action,
        "symbol": cmd.symbol,
        "type": cmd.type,
        "side": cmd.side,
        "entry": cmd.entry,
        "sl": cmd.sl,
        "tp": cmd.tp,
        "timestamp": int(time.time())
    }
    
    # Update or create order entry
    if order_id not in DB["orders"]:
        DB["orders"][order_id] = {
            "order_id": order_id,
            "first_msg_id": msg_id,
            "messages": [msg_id],
            "symbol": cmd.symbol,
            "side": cmd.side,
            "status": "active",
            "latest_action": cmd.action,
            "created_at": int(time.time()),
            "updated_at": int(time.time())
        }
    else:
        order = DB["orders"][order_id]
        if msg_id not in order["messages"]:
            order["messages"].append(msg_id)
        order["latest_action"] = cmd.action
        order["updated_at"] = int(time.time())
    
    save_db(DB)

def get_order_summary(order_id: str) -> Optional[Dict[str, Any]]:
    """Get summary of an order."""
    return DB["orders"].get(order_id)


# ---------- Parsing helpers ----------
PRICE_RE = r"(?:\d+(?:\.\d+)?)"

def normalize_symbol(raw: str) -> str:
    # Accept "CAD/CHF", "AUD/USD", "CADCHF", "AUDUSD"
    s = raw.strip().upper()
    s = s.replace(" ", "")
    s = s.replace("/", "")
    # Some signals might use "XAU/USD" or similar: becomes XAUUSD
    return f"{s}{SYMBOL_POSTFIX}"

def extract_symbol(text: str) -> Optional[str]:
    # Look for "CAD/CHF" style
    m = re.search(r"\b([A-Z]{3})\s*/\s*([A-Z]{3})\b", text.upper())
    if m:
        return normalize_symbol(m.group(1) + m.group(2))

    # Look for "AUD/USD" after BUY LIMIT line (already covered) or "AUDUSD"
    m = re.search(r"\b([A-Z]{6})\b", text.upper())
    if m:
        return normalize_symbol(m.group(1))

    return None

def extract_side(text: str) -> Optional[str]:
    t = text.upper()
    # Prefer explicit emoji lines: "📈BUY" / "📉SELL LIMIT"
    if "BUY" in t and "SELL" not in t:
        return "BUY"
    if "SELL" in t and "BUY" not in t:
        return "SELL"
    # If both appear, choose first occurrence
    i_buy = t.find("BUY")
    i_sell = t.find("SELL")
    if i_buy != -1 and i_sell != -1:
        return "BUY" if i_buy < i_sell else "SELL"
    return None

def extract_pending_type(text: str) -> Optional[OrderType]:
    t = text.upper()
    # Accept variants like "SELL LIMIT", "BUY LIMIT"
    if "BUY LIMIT" in t:
        return "BUY_LIMIT"
    if "SELL LIMIT" in t:
        return "SELL_LIMIT"
    if "BUY STOP" in t:
        return "BUY_STOP"
    if "SELL STOP" in t:
        return "SELL_STOP"
    return None

def extract_entry_sl_tp(text: str) -> Dict[str, float]:
    t = text.upper()

    # Entry: "Prezzo 0.66930" (di apertura) or similar
    entry = 0.0
    m = re.search(r"PREZZO\s+(" + PRICE_RE + r")", t)
    if m:
        entry = float(m.group(1))

    sl = 0.0
    m = re.search(r"STOP\s*LOSS.*?\b(" + PRICE_RE + r")\b", t, flags=re.DOTALL)
    if m:
        sl = float(m.group(1))

    tp = 0.0
    m = re.search(r"TAKE\s*PROFIT.*?\b(" + PRICE_RE + r")\b", t, flags=re.DOTALL)
    if m:
        tp = float(m.group(1))

    return {"entry": entry, "sl": sl, "tp": tp}

def parse_update(text: str) -> Optional[Dict[str, float]]:
    # (SELL LIMIT GBP/USD) - MODIFICARE IL PREZZO DI INGRESSO DA 1.33300 A  1.33100 ...
    t = text.upper().replace(",", ".")
    m = re.search(
        r"MODIFICARE\s+IL\s+PREZZO\s+DI\s+INGRESSO\s+DA\s+(" + PRICE_RE + r")\s+A\s+(" + PRICE_RE + r")",
        t
    )
    if not m:
        return None
    return {"old_entry": float(m.group(1)), "new_entry": float(m.group(2))}

def parse_cancel(text: str) -> Optional[Dict[str, Any]]:
    # ANNULLARE BUY LIMIT GBP/CHF   (1.03900 )✅
    t = text.upper().replace(",", ".")
    m = re.search(r"\bANNULLARE\b", t)
    if not m:
        return None

    ptype = extract_pending_type(t)
    side = extract_side(t)
    sym = extract_symbol(t)

    # Optional entry in parentheses
    entry = 0.0
    m2 = re.search(r"\(\s*(" + PRICE_RE + r")\s*\)", t)
    if m2:
        entry = float(m2.group(1))

    if not (ptype and side and sym):
        return None

    return {"symbol": sym, "type": ptype, "side": side, "entry": entry}

def is_market_direct(text: str) -> bool:
    t = text.upper()
    # Your template mentions: "BUY DIRETTA A MERCATO" / "OPERAZIONE IN BUY DIRETTA"
    # also hints: "ESECUZIONE A MERCATO" / "A MERCATO"
    return ("DIRETTA" in t and "MERCATO" in t) or ("ESECUZIONE A MERCATO" in t)

def should_ignore(text: str) -> bool:
    # You said "Other message ... can be ignored at the moment."
    # We only act if we match OPEN/UPDATE/CANCEL patterns.
    return False


# ---------- Main parser ----------
def parse_message(msg: Message) -> Optional[Command]:
    if not msg.message:
        return None
    raw = msg.message.strip()
    if not raw:
        return None

    text = raw.replace("\u00a0", " ")  # normalize NBSP
    t = text.upper().replace(",", ".")

    # Cancellation
    cancel = parse_cancel(t)
    if cancel:
        return Command(
            cmd_id=f"tg_{msg.id}",
            action="CANCEL",
            symbol=cancel["symbol"],
            type=cancel["type"],
            side=cancel["side"],
            entry=float(cancel.get("entry", 0.0)),
            sl=0.0,
            tp=0.0,
            old_entry=0.0,
            meta={"tg_id": msg.id, "kind": "cancel"}
        )

    # Update (entry price change)
    upd = parse_update(t)
    if upd:
        # Extract pending type and symbol from the header "(SELL LIMIT GBP/USD)" if present
        ptype = extract_pending_type(t) or "SELL_LIMIT"  # fallback
        side = extract_side(t) or ("SELL" if "SELL" in t else "BUY")
        sym = extract_symbol(t)
        if not sym:
            return None
        return Command(
            cmd_id=f"tg_{msg.id}",
            action="UPDATE",
            symbol=sym,
            type=ptype,
            side=side,
            entry=float(upd["new_entry"]),
            sl=0.0,   # keep same in EA
            tp=0.0,   # keep same in EA
            old_entry=float(upd["old_entry"]),
            meta={"tg_id": msg.id, "kind": "update"}
        )

    # OPEN: market direct vs pending
    sym = extract_symbol(t)
    if not sym:
        return None

    ptype = extract_pending_type(t)
    side = extract_side(t)
    if not side:
        return None

    values = extract_entry_sl_tp(t)

    if is_market_direct(t) or (ptype is None and "A MERCATO" in t):
        # MARKET open: entry is "now"; we keep entry=0 to indicate market execution
        return Command(
            cmd_id=f"tg_{msg.id}",
            action="OPEN",
            symbol=sym,
            type="MARKET",
            side=side,
            entry=0.0,
            sl=values["sl"],
            tp=values["tp"],
            old_entry=0.0,
            meta={"tg_id": msg.id, "kind": "market_direct"}
        )

    if ptype:
        # PENDING open: needs entry, sl, tp
        if values["entry"] <= 0:
            # no entry price found -> ignore (safer)
            return None
        return Command(
            cmd_id=f"tg_{msg.id}",
            action="OPEN",
            symbol=sym,
            type=ptype,
            side=side,
            entry=values["entry"],
            sl=values["sl"],
            tp=values["tp"],
            old_entry=0.0,
            meta={"tg_id": msg.id, "kind": "pending"}
        )

    return None


# ---------- Output writers ----------
CSV_HEADER = ["cmd_id", "order_id", "action", "symbol", "type", "side", "entry", "sl", "tp", "old_entry"]

def append_csv(cmd: Command, csv_path: Optional[str] = None) -> None:
    path = csv_path or CSV_PATH
    file_exists = os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if not file_exists:
            w.writerow(CSV_HEADER)
        w.writerow([
            cmd.cmd_id, cmd.order_id, cmd.action, cmd.symbol, cmd.type, cmd.side,
            f"{cmd.entry:.5f}" if cmd.entry else "0",
            f"{cmd.sl:.5f}" if cmd.sl else "0",
            f"{cmd.tp:.5f}" if cmd.tp else "0",
            f"{cmd.old_entry:.5f}" if cmd.old_entry else "0"
        ])

def append_jsonl(cmd: Command, jsonl_path: Optional[str] = None) -> None:
    path = jsonl_path or JSONL_PATH
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(asdict(cmd), ensure_ascii=False) + "\n")


# ---------- Telethon runner ----------
async def main(msg_ids: Optional[list] = None):
    if TG_API_ID == 0 or not TG_API_HASH:
        raise SystemExit("Missing TG_API_ID / TG_API_HASH in environment.")

    if not TG_GROUP:
        raise SystemExit("Missing TG_GROUP (group title, invite link, or ID).")

    client = TelegramClient(TG_SESSION, TG_API_ID, TG_API_HASH)

    # First run will ask for phone + code in console and create a session file.
    await client.start()
    print("✅ Logged in.")

    try:
        entity = await client.get_entity(TG_GROUP)
    except Exception as e:
        raise SystemExit(f"Could not resolve TG_GROUP '{TG_GROUP}'. Error: {e}")

    # Test mode: retrieve and process specific message IDs
    if msg_ids:
        print(f"📋 Test mode: processing {len(msg_ids)} message(s)...")
        for msg_id in msg_ids:
            try:
                msg = await client.get_messages(entity, ids=int(msg_id))
                if msg:
                    cmd = parse_message(msg)
                    if cmd:
                        # Check if message is a reply to another message
                        linked_msg_id = msg.reply_to_msg_id if hasattr(msg, 'reply_to_msg_id') else None
                        
                        if linked_msg_id:
                            # If message is a reply, retrieve order from the linked message
                            order_id = get_order_id_for_msg(linked_msg_id)
                            if order_id:
                                print(f"   📌 Reply to message {linked_msg_id}, retrieved order: {order_id}")
                            else:
                                # Linked message not found in DB, generate new order
                                order_id = f"order_msg{msg.id}"
                                print(f"   ⚠️  Reply to message {linked_msg_id} not in DB, generated order: {order_id}")
                        else:
                            # No reply/linked message, generate new order ID
                            order_id = f"order_msg{msg.id}"
                        
                        cmd.order_id = order_id
                        
                        # Link message to order in database
                        # This ensures future messages replying to this one can find the order directly
                        link_message_to_order(msg.id, order_id, cmd)
                        
                        # For UPDATE/CANCEL messages, explicitly log that they're stored as reference points
                        if cmd.action in ["UPDATE", "CANCEL"]:
                            print(f"   💾 Message {msg.id} stored as reference point for order {order_id}")
                        
                        # Create output files with message ID in the name
                        csv_path = os.path.join(OUT_DIR, f"copytrade_commands_msg{msg_id}.csv")
                        jsonl_path = os.path.join(OUT_DIR, f"copytrade_commands_msg{msg_id}.jsonl")
                        append_csv(cmd, csv_path)
                        append_jsonl(cmd, jsonl_path)
                        
                        order_info = get_order_summary(order_id)
                        print(f"🟢 Parsed msg {msg_id}: {cmd.cmd_id} {cmd.action} {cmd.symbol} {cmd.type} {cmd.side}")
                        print(f"   Order ID: {order_id}")
                        if order_info:
                            print(f"   Order Status: {order_info.get('latest_action')} ({len(order_info.get('messages', []))} message(s))")
                        print(f"   -> {csv_path}")
                        print(f"   -> {jsonl_path}")
                    else:
                        print(f"⚪ Message {msg_id} ignored (no valid command found)")
                else:
                    print(f"❌ Message {msg_id} not found")
            except Exception as e:
                print(f"❌ Error processing message {msg_id}: {e}")
        return

    # Normal mode: listen for new messages
    print("Listening…")

    @client.on(events.NewMessage(chats=entity))
    async def handler(event):
        msg = event.message
        if already_processed(msg.id):
            return

        cmd = parse_message(msg)
        if cmd:
            # Check if message is a reply to another message
            linked_msg_id = msg.reply_to_msg_id if hasattr(msg, 'reply_to_msg_id') else None
            
            if linked_msg_id:
                # If message is a reply, retrieve order from the linked message
                order_id = get_order_id_for_msg(linked_msg_id)
                if not order_id:
                    # Linked message not found in DB, generate new order
                    order_id = f"order_msg{msg.id}"
            else:
                # No reply/linked message, generate new order ID
                order_id = f"order_msg{msg.id}"
            
            cmd.order_id = order_id
            # Link message to order in database
            # This ensures future messages replying to this one can find the order directly
            link_message_to_order(msg.id, order_id, cmd)
            
            append_csv(cmd)
            append_jsonl(cmd)
            mark_processed(msg.id, int(time.time()))
            order_info = get_order_summary(order_id)
            print(f"🟢 Parsed: {cmd.cmd_id} {cmd.action} {cmd.symbol} {cmd.type} {cmd.side} [Order: {order_id}]")
            if order_info:
                print(f"   Status: {order_info.get('latest_action')} ({len(order_info.get('messages', []))} message(s))")
        else:
            # If you want to track ignored messages, uncomment:
            # print(f"⚪ Ignored msg {msg.id}")
            mark_processed(msg.id, int(time.time()))

    await client.run_until_disconnected()


if __name__ == "__main__":
    import asyncio
    
    parser = argparse.ArgumentParser(
        description="Parse Telegram trading signals. Optionally retrieve and process specific message IDs for testing.",
        epilog="Example: python script.py 280 281 282  # Process messages. Links are extracted from message content."
    )
    parser.add_argument(
        "msg_ids",
        nargs="*",
        help="Optional: Message IDs to retrieve and process (for testing). If not provided, listens for new messages."
    )
    
    args = parser.parse_args()
    
    # Convert message ID strings to integers
    msg_ids = [int(mid) for mid in args.msg_ids] if args.msg_ids else None
    
    asyncio.run(main(msg_ids=msg_ids))
