from common.utils import Bet

class ProtocolError(Exception):
    pass

class Protocol:
    """Define serialización y parsing de mensajes BET."""

    @staticmethod
    def escape(s: str) -> str:
        return s.replace("\\", "\\\\").replace("#", "\\#")

    @staticmethod
    def unescape(s: str) -> str:
        out, esc = [], False
        for ch in s:
            if esc:
                out.append(ch)
                esc = False
            elif ch == "\\":
                esc = True
            else:
                out.append(ch)
        if esc:
            out.append("\\")
        return "".join(out)

    @staticmethod
    def split_escaped(line: str, sep: str = "#") -> list[str]:
        parts, cur, esc = [], [], False
        for ch in line:
            if esc:
                cur.append(ch)
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == sep:
                parts.append("".join(cur))
                cur = []
            else:
                cur.append(ch)
        parts.append("".join(cur))
        return parts

    @classmethod
    def parse_bet(cls, line: str) -> Bet:
        parts = cls.split_escaped(line)
        if len(parts) != 7 or parts[0] != "BET":
            raise ProtocolError("invalid_format")
        _, agency, first, last, doc, birth, num = parts
        return Bet(
            cls.unescape(agency),
            cls.unescape(first),
            cls.unescape(last),
            cls.unescape(doc),
            cls.unescape(birth),
            cls.unescape(num),
        )

    @classmethod
    def serialize_bet(cls, bet: Bet) -> str:
        return "BET#{}#{}#{}#{}#{}#{}\n".format(
            cls.escape(str(bet.agency)),
            cls.escape(bet.first_name),
            cls.escape(bet.last_name),
            cls.escape(bet.document),
            cls.escape(bet.birthdate.isoformat()),
            cls.escape(str(bet.number)),
        )

    @staticmethod
    def ok() -> str:
        return "OK\n"
