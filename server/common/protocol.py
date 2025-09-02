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

    @classmethod
    def parse_batch(cls, message: str) -> list[Bet]:
        """
        Parse a BATCH message string into a list of Bet objects.
        
        Args:
            message (str): Raw BATCH message string in format "BATCH#count\nBET#...\nBET#..."
            
        Returns:
            list[Bet]: List of parsed bet objects
            
        Raises:
            ProtocolError: If message format is invalid or bet count doesn't match
        """
        lines = message.strip().split('\n')
        if not lines:
            raise ProtocolError("empty_message")
        
        header_parts = cls.split_escaped(lines[0])
        if len(header_parts) != 2 or header_parts[0] != "BATCH":
            raise ProtocolError("invalid_batch_header")
        
        try:
            expected_count = int(header_parts[1])
        except ValueError:
            raise ProtocolError("invalid_batch_count")
        
        bets = []
        for line in lines[1:]:
            line = line.strip()
            if not line:
                continue
            if not line.startswith("BET#"):
                raise ProtocolError("invalid_bet_line")
            bets.append(cls.parse_bet(line))
        
        if len(bets) != expected_count:
            raise ProtocolError(f"bet_count_mismatch: expected {expected_count}, got {len(bets)}")
        
        return bets

    @classmethod
    def parse_finish_bets(cls, line: str) -> str:
        """
        Parse a FINISH_BETS message to extract the agency ID.
        
        Args:
            line (str): Raw FINISH_BETS message string in format "FINISH_BETS#agency"
            
        Returns:
            str: Agency ID
            
        Raises:
            ProtocolError: If message format is invalid
        """
        parts = cls.split_escaped(line)
        if len(parts) != 2 or parts[0] != "FINISH_BETS":
            raise ProtocolError("invalid_finish_bets_format")
        return cls.unescape(parts[1])

    @classmethod
    def parse_query_winners(cls, line: str) -> str:
        """
        Parse a QUERY_WINNERS message to extract the agency ID.
        
        Args:
            line (str): Raw QUERY_WINNERS message string in format "QUERY_WINNERS#agency"
            
        Returns:
            str: Agency ID
            
        Raises:
            ProtocolError: If message format is invalid
        """
        parts = cls.split_escaped(line)
        if len(parts) != 2 or parts[0] != "QUERY_WINNERS":
            raise ProtocolError("invalid_query_winners_format")
        return cls.unescape(parts[1])

    @classmethod
    def serialize_winners(cls, winners: list[str]) -> str:
        """
        Serialize a list of winner documents into a WINNERS message string.
        
        Args:
            winners (list[str]): List of winner document numbers
            
        Returns:
            str: Formatted WINNERS message string
        """
        escaped_winners = [cls.escape(doc) for doc in winners]
        return "WINNERS#{}#{}\n".format(len(winners), "#".join(escaped_winners))

