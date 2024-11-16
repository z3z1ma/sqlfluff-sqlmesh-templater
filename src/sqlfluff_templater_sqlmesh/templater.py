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
from sqlglot.tokens import Tokenizer, TokenType

if t.TYPE_CHECKING:  # pragma: no cover
    from sqlfluff.cli.formatters import OutputStreamFormatter
    from sqlfluff.core import FluffConfig

templater_logger = logging.getLogger("sqlfluff.templater")


def _process_select_statement(
    select_statement: str, select_start: int
) -> t.Tuple[str, t.List[RawFileSlice], t.List[TemplatedFileSlice]]:
    """Process the SELECT statement, removing '@' symbols and handling macro func calls.

    Args:
        select_statement: The SELECT statement string from the source SQL.
        select_start: The starting index of the SELECT statement in the source SQL.

    Returns:
        A tuple containing the processed SELECT statement, a list of RawFileSlice,
        and a list of TemplatedFileSlice for accurate mapping.
    """
    output: t.List[str] = []

    pos, tpl_pos = 0, 0
    raw_slices, tpl_slices = [], []

    current_slice_type = None
    current_raw_slice_start = 0
    current_tpl_slice_start = 0

    def _start_new_slice(slice_type: str, source_idx: int, templated_idx: int) -> None:
        """Start a new slice.

        Args:
            slice_type: The type of the slice (e.g., 'literal', 'templated').
            source_idx: The starting index of the slice in the source SQL.
            templated_idx: The starting index of the slice in the templated SQL.
        """
        nonlocal current_slice_type, current_raw_slice_start, current_tpl_slice_start
        current_slice_type = slice_type
        current_raw_slice_start = source_idx
        current_tpl_slice_start = templated_idx

    def _end_current_slice(source_idx: int, templated_idx: int) -> None:
        """End the current slice.

        Args:
            source_idx: The ending index of the slice in the source SQL.
            templated_idx: The ending index of the slice in the templated SQL.
        """
        nonlocal current_slice_type, current_raw_slice_start, current_tpl_slice_start
        if current_slice_type is not None:
            raw_slice = select_statement[
                current_raw_slice_start - select_start : source_idx - select_start
            ]
            raw_slices.append(
                RawFileSlice(
                    raw=raw_slice,
                    slice_type=current_slice_type,
                    source_idx=current_raw_slice_start,
                )
            )
            tpl_slices.append(
                TemplatedFileSlice(
                    slice_type=current_slice_type,
                    source_slice=slice(current_raw_slice_start, source_idx),
                    templated_slice=slice(current_tpl_slice_start, templated_idx),
                )
            )
            current_slice_type = None
            current_raw_slice_start = None
            current_tpl_slice_start = None

    while pos < len(select_statement):
        c = select_statement[pos]
        source_idx = select_start + pos

        if c == "@":
            # End current literal slice before '@'
            _end_current_slice(source_idx, tpl_pos)

            # Process '@' symbol (removed in output)
            # Create a templated slice for '@' with zero length in output
            raw_slices.append(
                RawFileSlice(raw="@", slice_type="templated", source_idx=source_idx)
            )
            tpl_slices.append(
                TemplatedFileSlice(
                    slice_type="templated",
                    source_slice=slice(source_idx, source_idx + 1),
                    templated_slice=slice(tpl_pos, tpl_pos),
                )
            )
            pos += 1  # Skip '@'

            # Start a new literal slice after '@'
            _start_new_slice("literal", source_idx + 1, tpl_pos)

            # Collect identifier (function name or variable name)
            ident_start = pos
            while pos < len(select_statement) and (
                select_statement[pos].isalnum() or select_statement[pos] == "_"
            ):
                pos += 1
            ident_end = pos
            ident = select_statement[ident_start:ident_end]
            output.append(ident)
            tpl_pos += len(ident)

            # Collect any whitespace (include in current literal slice)
            while pos < len(select_statement) and select_statement[pos].isspace():
                output.append(select_statement[pos])
                tpl_pos += 1
                pos += 1

            # Check if it's a function call
            if pos < len(select_statement) and select_statement[pos] == "(":
                # Include '(' in current literal slice
                output.append("(")
                tpl_pos += 1
                pos += 1

                # End current literal slice before function arguments
                _end_current_slice(select_start + pos, tpl_pos)

                # Process function arguments (replace with 'PLACEHOLDER')
                func_args_start = pos
                paren_count = 1
                while pos < len(select_statement) and paren_count > 0:
                    if select_statement[pos] == "(":
                        paren_count += 1
                    elif select_statement[pos] == ")":
                        paren_count -= 1
                    pos += 1
                func_args_end = pos - 1  # Index of the closing ')'

                func_args_source_start = select_start + func_args_start
                func_args_source_end = select_start + func_args_end

                # Replace function arguments with 'PLACEHOLDER'
                placeholder = "'PLACEHOLDER'"
                raw_slices.append(
                    RawFileSlice(
                        raw=select_statement[func_args_start:func_args_end],
                        slice_type="templated",
                        source_idx=func_args_source_start,
                    )
                )
                tpl_slices.append(
                    TemplatedFileSlice(
                        slice_type="templated",
                        source_slice=slice(
                            func_args_source_start, func_args_source_end
                        ),
                        templated_slice=slice(tpl_pos, tpl_pos + len(placeholder)),
                    )
                )
                output.append(placeholder)
                tpl_pos += len(placeholder)

                # Process closing ')'
                c = select_statement[func_args_end]
                source_idx = select_start + func_args_end
                _start_new_slice("literal", source_idx, tpl_pos)
                output.append(c)
                tpl_pos += 1
                pos = func_args_end + 1
            else:
                # Not a function call; continue in current literal slice
                pass
        else:
            # Regular character
            if current_slice_type != "literal":
                # Start new literal slice
                _start_new_slice("literal", source_idx, tpl_pos)
            output.append(c)
            tpl_pos += 1
            pos += 1

    # End any remaining slice
    _end_current_slice(select_start + pos, tpl_pos)

    processed_select = "".join(output)
    return processed_select, raw_slices, tpl_slices


def _process_sql_script(
    input_str: str, fname: str = "<string>", *, dialect: str
) -> TemplatedFile:
    """Process the entire SQL script, extracting and processing the SELECT statement.

    Args:
        input_str: The entire SQL script as a string.
        fname: The filename of the SQL script.
        dialect: The SQLGlot dialect name.

    Returns:
        A TemplatedFile object with accurate slices mapping between source and templated content.
    """
    tokenizer = Tokenizer(dialect=dialect)
    tokens = tokenizer.tokenize(input_str)

    select_start_pos = None
    select_end_pos = None

    select_discovered = False
    for _, token in enumerate(tokens):
        if token.token_type == TokenType.SELECT and not select_discovered:
            select_start_pos = token.start
            select_discovered = True
        elif select_discovered:
            if token.token_type == TokenType.SEMICOLON:
                select_end_pos = token.end
                break
    if select_discovered and select_end_pos is None:
        select_end_pos = len(input_str)
    elif not select_discovered:
        raise SQLFluffSkipFile("No SELECT statement found in file", fname)

    if t.TYPE_CHECKING:
        assert select_start_pos is not None and select_end_pos is not None

    select_statement = input_str[select_start_pos:select_end_pos]

    (
        select_processed,
        select_raw_slices,
        select_templated_slices,
    ) = _process_select_statement(select_statement, select_start_pos)

    raw_slices = []
    templated_slices = []
    templated_pos = 0

    if select_start_pos > 0:
        raw_slices.append(
            RawFileSlice(
                raw=input_str[0:select_start_pos],
                slice_type="templated",
                source_idx=0,
            )
        )
        templated_slices.append(
            TemplatedFileSlice(
                slice_type="templated",
                source_slice=slice(0, select_start_pos),
                templated_slice=slice(0, 0),
            )
        )

    raw_slices.extend(select_raw_slices)
    templated_slices.extend(select_templated_slices)
    templated_pos += len(select_processed)

    if select_end_pos < len(input_str):
        raw_slices.append(
            RawFileSlice(
                raw=input_str[select_end_pos:],
                slice_type="templated",
                source_idx=select_end_pos,
            )
        )
        templated_slices.append(
            TemplatedFileSlice(
                slice_type="templated",
                source_slice=slice(select_end_pos, len(input_str)),
                templated_slice=slice(templated_pos, templated_pos),
            )
        )

    f = TemplatedFile(
        source_str=input_str,
        fname=fname,
        templated_str=select_processed,
        sliced_file=templated_slices,
        raw_sliced=raw_slices,
    )
    return f


class SQLMeshTemplater(RawTemplater):
    """A templater using sqlmesh."""

    name = "sqlmesh"

    def __init__(
        self,
        override_context: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> None:
        self.working_dir: str = os.getcwd()
        self.tokenizer = Tokenizer()
        super().__init__(override_context=override_context)

    def config_pairs(self) -> t.List[t.Tuple[str, str]]:
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
        if t.TYPE_CHECKING:
            assert formatter

        context = self.get_context(fname, config)

        fname = os.path.abspath(fname) if fname != "stdin" else fname
        original_file_path = os.path.relpath(fname, start=os.getcwd())

        if not in_str:
            raise SQLFluffSkipFile(f"Skipping empty file: {original_file_path}", fname)

        dialect = context.get("sqlglot_dialect", None)
        return _process_sql_script(in_str, fname, dialect=dialect), []
