module SimpleSQL
using Serialization  # for file-backed datastores.

# SQL query-style API
export @SQL, @CREATE, @INSERT, @SELECT, @WHERE, @GROUP
# julia function-style api.
export create_table, insert_into_values, select_from
# Disk i/o
export write_table_to_disk, read_table_from_disk

struct Table
    title::String
    headers
    cols
end
""" users = create_table("users", (:id, Int64), (:name, String)) """
create_table(title, key_pairs...) =
    create_table(title, collect((Symbol(s),t) for (s,t) in key_pairs))
function create_table(title, key_pairs::Vector{Tuple{Symbol, DataType}})
    cols = Tuple((Vector{T}() for (s,T) in key_pairs))
    return Table(title, [k for (k,v) in key_pairs], cols)
end

function write_table_to_disk(table, filename)
    if !isempty(filename)
        io = open(filename, "w")
        serialize(io, table)
        close(io)
    end
end
function read_table_from_disk(filename)
    if !isempty(filename)
        io = open(filename, "r")
        t = deserialize(io)
        close(io)
        return t
    end
end

tostring(x) = join([x], "")  # String(::Expr) fails for reasons..
function Base.show(io::IO, t::Table)
    str = t.title *"\n"*
        " "*join(t.headers, "\t| ") *"\n"*
        join(["-"^(length(tostring(h))+1) for h in t.headers], "\t|") *"\n";
        # TODO: i'm only printing up to the minimum length of a column, for things like `SELECT id, SUM(id) from t`
    num_cols = minimum(length.(t.cols))
    num_cols_capped = min(15, num_cols)  # cap out so you don't freeze printing.
    str *= join([" "*join([c[i] for c in t.cols], "\t| ") for i in 1:num_cols_capped],"\n") *"\n"
    if (num_cols_capped < num_cols)
        str *= join(["..." for h in t.headers], "\t|")
    end
    print(io, str)
end

""" insert_into_values(users, (1, "nathan")) """
function insert_into_values(t, values)
    table = t.cols
    for i in 1:length(values)
        push!(table[i], values[i])
    end
end

# The main `SELECT` function. This is the interface, and all specializations for
# different types happen via specializations of _select_from_internal.
"""
    _select_from(users, :id, :(uppercase.(name)))
 Perform a SELECT query on t. Queries can be column names or expressions.
 """
function _select_from(t, columns...)
    out_names, out_cols = [], []
    for c in columns
        r = _select_from_internal(t, c)
        push!(out_names, r.headers)
        push!(out_cols, r.cols)
    end
    Table("RESULTS", Tuple(Iterators.flatten(out_names)), Tuple(Iterators.flatten(out_cols)))
end
_select_from(t, columns::Tuple) = _select_from(t, columns...)
_select_from(t, columns::Vector) = _select_from(t, columns...)

# Use compiler dispatch to compare column names w/ :*
matches(a::Val{S}, b::Val{S}) where {S} = true
matches(a::Val{A}, b::Val{B}) where {A, B} = false
matches(a::Val{A}, b::Val{:*}) where {A} = true
matches(a::Val{:*}, b::Val{A}) where {A} = true
# For a single symbol, just find the matching column name.
function _select_from_internal(t, col::Symbol)
    indices = Tuple(Iterators.flatten(findall(s -> matches(Val(s), Val(col)), t.headers)))
    length(indices) >= 1 || error("Column $col does not exist.")
    Table("RESULTS", Tuple(t.headers[i] for i in indices), Tuple(t.cols[i] for i in indices))
end
_select_from_internal(t, c::String) = _select_from_internal(t, Symbol(c))
_select_from_internal(t, expr::QuoteNode) = _select_from_internal(t, expr.value)

# For expressions, we evaluate the expression with the column name replaced with
# its value. This requires digging into the expression to find the column name,
# retrieving the column(s), and then evaluating the expr with the column(s) value(s).
function _select_from_internal(t, expr::Expr)
    col, sym = expr_to_col(expr)
    val_table = _select_from(t, col)
    val = val_table.cols
    return _eval_expr_internal(val,sym, expr)
end
function _eval_expr_internal(val,sym, expr::Expr)
    out_colname = tostring(expr)  # Before mutating expr.
    global ____select_from_col = permutedims(reshape(collect(Iterators.flatten(val)), length(val),:))
    sym[] = ____select_from_col
    r = eval(expr)
    # Now turn the 3x1 Arrays back into a 3-el Vector
    if length(size(r)) == 2 && size(r)[end] == 1
        r = reshape(r, size(r)[1])
    elseif size(r) == ()
        r = [r]  # wrap scalar results as a 1-vector
    end
    Table("RESULTS", [out_colname], (r,))
end
# Convert :(sum(id)) to :id, :(sum(<placeholder>)) so that the column can be
#  retrieved and its value inserted into the expr.
function expr_to_cols(expressions::Vector{Expr})
    cols, syms = Vector{Symbol}(), Vector{Base.RefArray}()
    for expr in expressions
        c, s = expr_to_col(expr)
        push!(cols, c); push!(syms, s);
    end
    cols,syms
end
function expr_to_col(expr::Expr)
    head = expr.head
    if head == :call
        column = expr.args[2]
        ref = Ref(expr.args, 2)
    elseif head == :.
        column = expr.args[2].args[1]
        ref = Ref(expr.args[2].args, 1)
    end
    return column, ref
end

struct SelectResult
    headers
    cols::Matrix
end
import Base: ==
==(a::SelectResult, b::SelectResult) = (a.headers == b.headers && a.cols == b.cols)

function Base.show(io::IO, t::SelectResult)
    println(io, join(t.headers, " | ") *"\n"*
                join(["-"^(length(tostring(h))+1) for h in t.headers], "---"));
    Base.print_matrix(io, t.cols,
                      " ", " | ")
end

select_from(t, columns::Tuple; where=nothing, groupby=nothing) = select_from(t, columns...; where=where, groupby=groupby)
select_from(t, columns::Vector; where=nothing, groupby=nothing) = select_from(t, columns...; where=where, groupby=groupby)
function select_from(t, colexprs...; where=nothing, groupby=nothing)
    if where == nothing && groupby == nothing
        out_table = _select_from(t, colexprs...)
        out_colnames = out_table.headers
        results = out_table.cols
    else
        if where != nothing
            wherecolexpr = _retrieve_col_name(where)
            val_table = _select_from(t, wherecolexpr.name)
            vals = val_table.cols
            sym, expr = wherecolexpr.sym, wherecolexpr.expr
            # Get bitarray for whereexpr row-filter.
            rowfilter = _eval_expr_internal(vals, sym, expr)
        end
        if groupby != nothing
            grouped_table = _select_from(t, groupby)
            grouping_vals = grouped_table.cols
            @assert length(grouping_vals) == 1
            colors, counts = color_unique_vals(grouping_vals[1])
            num_colors = maximum(colors)
        end

        # Now get the actual values for the colexprs. This works as follows:
        #  1. Get each column based on the symbol, or the name in an expression.
        #  2. Apply `where` filter if present.
        #  3. Create new grouped columns based on `groupby` clause if present.
        #  4. Eval any expressions present in the resultant columns.
        results = []
        out_colnames = []
        for colexpr in colexprs
            col = _retrieve_col_name(colexpr)
            val_table = _select_from(t, col.name)
            val = val_table.cols
            # Now filter with where
            if where != nothing
                filtered = map(c->getindex(c, rowfilter.cols[1]), val)
            else
                filtered = val
            end
            # Then finally eval the expressions
            if groupby != nothing
                inner_results, inner_colnames = _select_from__group_by_internal(t, col, colors, counts, num_colors)
            else
                if isa(col, ColumnExprRef)
                    sym = col.sym
                    t = _eval_expr_internal(filtered, sym, colexpr)
                    inner_colnames, inner_results = t.headers, t.cols
                else
                    inner_colnames, inner_results = val_table.headers, filtered
                end
            end
            push!(results, inner_results...)
            push!(out_colnames, inner_colnames...)
        end
    end
    # Trim all columns to the same length and return results as a Matrix.
    minlen = minimum(length.(results))
    return SelectResult(out_colnames, hcat(collect(r[end-minlen+1:end] for r in results)...))
end

# Types for dispatching to handle columns as expression.
struct Column
    name::Symbol
end
struct ColumnExprRef
    name::Symbol
    sym
    expr::Expr
end
_retrieve_col_name(col::Symbol) = Column(col)
_retrieve_col_name(expr::Expr) = ColumnExprRef(expr_to_col(expr)..., expr)
_retrieve_col_names(colexprs...) = [_retrieve_col_name(col) for col in colexprs]

# -- Groupby implementation
# Rename each unique item to a unique number, and get counts.
# Eg: convert (5,7,7,2,7) -> (1,2,2,3,2) and [1,3,1]
function color_unique_vals(col::Array{T,1}) where T
    seen = Dict{T, Int64}()
    cur = 1
    out = copy(col)
    counts = []
    for (i,v) in enumerate(col)
        if !(v in keys(seen))
            seen[v] = cur
            push!(counts, 0)
            cur += 1
        end
        out[i] = seen[v]
        counts[seen[v]] += 1
    end
    out, counts
end

function _select_from__group_by_internal(t, colexpr, colors, counts, num_colors)
    vals_table = _select_from(t, colexpr.name)
    vals = vals_table.cols

    results = []
    out_colnames = _colnames_from_table(vals_table, colexpr)
    for v in vals
        splits = [Vector{eltype(v)}(undef, c) for c in counts]
        for i in 1:num_colors
            splits[i] .= v[colors.==i]
        end

        row_results = []
        for val in splits
            push!(row_results, _get_val_from_split(val, colexpr)...)
        end
        push!(results, row_results)
    end
    return results, out_colnames
end

_colnames_from_table(t, colexpr::Column) = t.headers
_colnames_from_table(t, colexpr::ColumnExprRef) = [tostring(colexpr.expr)]

function _get_val_from_split(v, colexpr::Column)
    return [v[1]]
end
function _get_val_from_split(v, colexpr::ColumnExprRef)
    sym, expr = colexpr.sym, colexpr.expr
    out_t = _eval_expr_internal(v, sym, expr)
    return out_t.cols[1]
end


# ----------------------- Now start the SQL interpreter part --------------

macro CREATE(expr)
    @assert(expr.head == :macrocall)
    @assert(expr.args[1] == Symbol("@TABLE"))
    # args[2] is @TABLE's LineNumberNode
    table = expr.args[3]
    name = String(table)
    colpairs = eval(expr.args[4])
    # Escape expr so table name doesn't become a weird macro-local variable.
    esc(quote
        $table = create_table($name, $(colpairs...))
    end)
end

macro INSERT(expr)
    @assert(expr.head == :macrocall)
    @assert(expr.args[1] == Symbol("@INTO"))
    # args[2] is @TABLE's LineNumberNode
    table = expr.args[3]
    values = expr.args[4]
    @assert(values.args[1] == Symbol("@VALUES"))
    vals = eval(values.args[3])
    quote
        insert_into_values($(esc(table)), $vals)
    end
end

quote_column_syms(colexpr) = colexpr
function quote_column_syms(colexpr::Symbol)
    QuoteNode(colexpr)
end
function quote_column_syms(colexpr::Expr)
    if colexpr.head == :tuple
        for (i,v) in enumerate(colexpr.args)  # re-quote bare symbols (for @SELECT id, aisle @FROM t)
            if v isa Symbol
                colexpr.args[i] = QuoteNode(v)
            elseif v isa Expr && v.head != :quote
                colexpr.args[i] = Expr(:quote, v)
            end
        end
    elseif colexpr.head != :quote
        colexpr = Expr(:quote, colexpr)
    end
    colexpr
end
function macro_select(colexpr, extraexpr::Expr)
    parsed_exprs = parse_macro_block(colexpr, extraexpr)
    colexpr, fromexpr, keyargs = parsed_exprs[1], parsed_exprs[2], parsed_exprs[3:end]
    colexpr = quote_column_syms(colexpr)
    @assert(fromexpr.args[1] == Symbol("@FROM"))
    return quote
        select_from($(esc(fromexpr.args[3])), $(esc(colexpr)); $(keyargs...))
    end
end
macro SELECT(colexpr, fromexpr::Expr)
    return macro_select(colexpr, fromexpr)
end

# Handle from start of @-section to next @-section
function parse_macro_block(expr)
    #return expr
    return _macro_block(Val(expr.args[1]), expr)
end
function parse_macro_block(expr, extraexpr)
    #res = expr
    if expr isa Union{Symbol, QuoteNode} || expr.head == :tuple
        res = expr
    else
        res = _macro_block(Val(expr.args[1]), expr)
    end
    if length(extraexpr.args) > 3
        nextexpr = extraexpr.args[4]
        deleteat!(extraexpr.args, 4)
        return res, parse_macro_block(extraexpr, nextexpr)...
    end
    return res, _macro_block(Val(extraexpr.args[1]), extraexpr)
end


function _macro_block(::Val{Symbol("@GROUP")}, expr)
    @assert expr.args[3].args[1] == Symbol("@BY")
    groupcolexpr = quote_column_syms(expr.args[3].args[3])
    esc(:(groupby=$groupcolexpr))
end
function _macro_block(::Val{Symbol("@WHERE")}, expr)
    whereexpr = quote_column_syms(expr.args[3])
    esc(:(where=$whereexpr))
end
function _macro_block(::Val{S}, expr) where S
    expr
end


"""
        @SQL begin
            @SELECT ...
            @FROM ...
        end
    Macro to create multiline SQL statements. Note that each internal macro must
    have at least one argument on the same line (e.g. Can't do `@SELECT
                                                                  id`).
 """
macro SQL(expr)
    @assert expr.head == :block
    # Squash everything into a recursive expr, to match how it would be on a single line.
    inside_expr = expr.args[2]
    while length(expr.args) > 2
        push!(inside_expr.args, expr.args[4])
        inside_expr = inside_expr.args[end]
        deleteat!(expr.args, (3,4))
    end
    # Return the @SELECT expression.
    esc(expr.args[2])
end

end