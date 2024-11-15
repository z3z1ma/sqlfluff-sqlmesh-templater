"""Defines a SQLMesh templater."""

import logging
import os
import os.path
import typing as t

from sqlfluff.core.errors import SQLFluffSkipFile, SQLFluffUserError, SQLTemplaterError
from sqlfluff.core.templaters.base import (
    RawFileSlice,
    RawTemplater,
    TemplatedFile,
    TemplatedFileSlice,
    large_file_check,
)

if t.TYPE_CHECKING:  # pragma: no cover
    from sqlfluff.cli.formatters import OutputStreamFormatter
    from sqlfluff.core import FluffConfig

templater_logger = logging.getLogger("sqlfluff.templater")


# TODO: a basic no dependency tokenizer, but we will 90% likely replace this with sqlglot
# if it has the positions we need.
def _tokenize_sql(input_str: str) -> t.List[t.Tuple[str, str, int, int]]:
    """Basic and simple tokenization of a SQL script."""
    tokens = []
    i = 0
    while i < len(input_str):
        c = input_str[i]
        if input_str[i : i + 2] == "--":
            # Single-line comment
            start_idx = i
            i += 2
            while i < len(input_str) and input_str[i] != "\n":
                i += 1
            if i < len(input_str):
                i += 1  # Include the newline
            tokens.append(("COMMENT", input_str[start_idx:i], start_idx, i))
        elif input_str[i : i + 2] == "/*":
            # Multi-line comment
            start_idx = i
            i += 2
            while i < len(input_str) and input_str[i : i + 2] != "*/":
                i += 1
            if i < len(input_str):
                i += 2  # Include '*/'
            tokens.append(("COMMENT", input_str[start_idx:i], start_idx, i))
        elif c == "'":
            # Single-quoted string
            start_idx = i
            i += 1
            while i < len(input_str):
                if input_str[i] == "'":
                    i += 1
                    if i < len(input_str) and input_str[i] == "'":
                        # Escaped quote
                        i += 1
                    else:
                        break
                else:
                    i += 1
            tokens.append(("STRING", input_str[start_idx:i], start_idx, i))
        elif c == '"':
            # Double-quoted string
            start_idx = i
            i += 1
            while i < len(input_str):
                if input_str[i] == '"':
                    i += 1
                    if i < len(input_str) and input_str[i] == '"':
                        # Escaped quote
                        i += 1
                    else:
                        break
                else:
                    i += 1
            tokens.append(("STRING", input_str[start_idx:i], start_idx, i))
        elif c.isspace():
            # Whitespace
            start_idx = i
            while i < len(input_str) and input_str[i].isspace():
                i += 1
            tokens.append(("WHITESPACE", input_str[start_idx:i], start_idx, i))
        elif c.isalpha() or c == "_":
            # Identifier or keyword
            start_idx = i
            while i < len(input_str) and (
                input_str[i].isalnum() or input_str[i] == "_"
            ):
                i += 1
            tokens.append(("IDENTIFIER", input_str[start_idx:i], start_idx, i))
        else:
            # Other symbols
            tokens.append(("SYMBOL", c, i, i + 1))
            i += 1
    return tokens


def _process_tokens(
    tokens: t.List[t.Tuple[str, str, int, int]],
) -> t.Tuple[str, t.List[RawFileSlice], t.List[TemplatedFileSlice]]:
    """Process our simple tokens from a SQL script producing file slices."""
    output_tokens = []
    raw_sliced = []
    sliced_file = []

    templated_idx = 0
    first_select_seen = False
    select_mode = False  # Indicates we're processing the SELECT statement

    for token in tokens:
        token_type, token_value, start_idx, end_idx = token
        if token_type in ("COMMENT", "STRING", "WHITESPACE", "SYMBOL", "IDENTIFIER"):
            if (
                not first_select_seen
                and token_type == "IDENTIFIER"
                and token_value.lower() == "select"
            ):
                # Found SELECT statement
                first_select_seen = True
                select_mode = True
            if select_mode:
                if token_type == "SYMBOL" and token_value == ";":
                    # End of SELECT statement
                    select_mode = False
                if select_mode:
                    if token_type == "SYMBOL" and token_value == "@":
                        # Remove '@' symbol
                        raw_slice = RawFileSlice(
                            raw=token_value,
                            slice_type="templated",
                            source_idx=start_idx,
                        )
                        raw_sliced.append(raw_slice)
                        templated_slice = TemplatedFileSlice(
                            slice_type="templated",
                            source_slice=slice(start_idx, end_idx),
                            templated_slice=slice(templated_idx, templated_idx),
                        )
                        sliced_file.append(templated_slice)
                    else:
                        # Include token in output
                        raw_slice = RawFileSlice(
                            raw=token_value, slice_type="literal", source_idx=start_idx
                        )
                        raw_sliced.append(raw_slice)
                        templated_slice = TemplatedFileSlice(
                            slice_type="literal",
                            source_slice=slice(start_idx, end_idx),
                            templated_slice=slice(
                                templated_idx, templated_idx + len(token_value)
                            ),
                        )
                        sliced_file.append(templated_slice)
                        output_tokens.append(token_value)
                        templated_idx += len(token_value)
                else:
                    # After the SELECT statement has ended (after the semicolon)
                    raw_slice = RawFileSlice(
                        raw=token_value, slice_type="templated", source_idx=start_idx
                    )
                    raw_sliced.append(raw_slice)
                    templated_slice = TemplatedFileSlice(
                        slice_type="templated",
                        source_slice=slice(start_idx, end_idx),
                        templated_slice=slice(templated_idx, templated_idx),
                    )
                    sliced_file.append(templated_slice)
            else:
                # Before SELECT or after SELECT has ended
                raw_slice = RawFileSlice(
                    raw=token_value, slice_type="templated", source_idx=start_idx
                )
                raw_sliced.append(raw_slice)
                templated_slice = TemplatedFileSlice(
                    slice_type="templated",
                    source_slice=slice(start_idx, end_idx),
                    templated_slice=slice(templated_idx, templated_idx),
                )
                sliced_file.append(templated_slice)
        else:
            pass

    output_str = "".join(output_tokens)
    return output_str, raw_sliced, sliced_file


def process_sql_script(input_str: str, fname: str = "stdin") -> TemplatedFile:
    """Process a SQL script."""
    tokens = _tokenize_sql(input_str)
    output_str, raw_sliced, sliced_file = _process_tokens(tokens)
    return TemplatedFile(
        source_str=input_str,
        fname=fname,
        templated_str=output_str,
        sliced_file=sliced_file,
        raw_sliced=raw_sliced,
    )


class SQLMeshTemplater(RawTemplater):
    """A templater using sqlmesh."""

    name = "sqlmesh"

    def __init__(
        self,
        override_context: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> None:
        self.working_dir: str = os.getcwd()
        super().__init__(override_context=override_context)

    def config_pairs(self):
        """Returns info about the given templater for output by the cli."""
        return [("templater", self.name)]

    @large_file_check
    def process(
        self,
        *,
        in_str: str,
        fname: str,
        config: t.Optional["FluffConfig"] = None,
        formatter: t.Optional["OutputStreamFormatter"] = None,
    ) -> t.Tuple[TemplatedFile, t.List[SQLTemplaterError]]:
        """Compile a sqlmesh model and return the compiled SQL.

        Args:
            fname: Path to sqlmesh model(s)
            in_str: fname contents using configured encoding
            config: A specific config to use for this
                templating operation. Only necessary for some templaters.
            formatter: Optional object for output.
        """
        if config is None:
            raise SQLFluffUserError(
                "A configuration object must be provided to the templater."
            )

        fname = os.path.abspath(fname) if fname != "stdin" else fname
        original_file_path = os.path.relpath(fname, start=os.getcwd())

        if not in_str:
            raise SQLFluffSkipFile(f"Skipping empty file: {original_file_path}", fname)

        return (
            process_sql_script(in_str, fname),
            [],
        )
