from common.utils import Bet

class ProtocolError(Exception):
    pass

class Protocol:
    """
    Protocol handler for BET message serialization and parsing.
    
    This class defines the communication protocol between lottery clients and server.
    Messages use '#' as delimiter to separate fields in BET format:
    BET#agency#first_name#last_name#document#birthdate#number
    """

    @staticmethod
    def escape(s: str) -> str:
        """
        Escape special characters in a string for safe transmission.
        
        Args:
            s (str): Input string to escape
            
        Returns:
            str: Escaped string with backslashes and hash symbols properly escaped
        """
        return s.replace("\\", "\\\\").replace("#", "\\#")

    @staticmethod
    def unescape(s: str) -> str:
        """
        Unescape a previously escaped string to restore original content.
        
        Args:
            s (str): Escaped string to process
            
        Returns:
            str: Unescaped string with original content restored
        """
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
        """
        Split a string by separator while respecting escaped characters.
        
        Args:
            line (str): Input string to split
            sep (str): Separator character (default: '#')
            
        Returns:
            list[str]: List of split parts with escaped characters properly handled
        """
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
        """
        Parse a BET message string into a Bet object.
        
        Args:
            line (str): Raw BET message string in format "BET#agency#first#last#doc#birth#num"
            
        Returns:
            Bet: Parsed bet object with all fields properly typed
            
        Raises:
            ProtocolError: If message format is invalid or doesn't contain exactly 7 parts
        """
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
        """
        Serialize a Bet object into a BET message string.
        
        Args:
            bet (Bet): Bet object to serialize
            
        Returns:
            str: Formatted BET message string with newline terminator
        """
        return "BET#{}#{}#{}#{}#{}#{}\n".format(
            cls.escape(str(bet.agency)),
            cls.escape(bet.first_name),
            cls.escape(bet.last_name),
            cls.escape(bet.document),
            cls.escape(bet.birthdate.isoformat()),
            cls.escape(str(bet.number)),
        )

