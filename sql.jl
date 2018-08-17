module SQL
using Serialization  # for file-backed datastores.

# SQL query-style API
export @SQL, @CREATE, @INSERT, @SELECT
# julia function-style api.
export create_table, insert_into_values, select_from, select_from__group_by
# Disk i/o
export write_table_to_disk, read_table_from_disk

struct Table
    title::String
    headers
    cols
    #backing_file  # if not empty, this data is stored on disk in this file
    #Table(title,headers,cols) = new(title,headers,cols, "")
    #Table(title,headers,cols,backing_file) = new(title,headers,cols,backing_file)
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
    select_from(users, :id, :(uppercase.(name)))
 Perform a SELECT query on t. Queries can be column names or expressions.
 """
function select_from(t, columns...)
    out_names, out_cols = [], []
    for c in columns
        r = _select_from_internal(t, c)
        push!(out_names, r.headers)
        push!(out_cols, r.cols)
    end
    Table("RESULTS", Tuple(Iterators.flatten(out_names)), Tuple(Iterators.flatten(out_cols)))
end
select_from(t, columns::Tuple) = select_from(t, columns...)
select_from(t, columns::Vector) = select_from(t, columns...)

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
    val_table = select_from(t, col)
    val = val_table.cols
    return _eval_expr_internal(val,sym, expr)
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
function _eval_expr_internal(val,sym, expr::Expr)
    out_colname = tostring(expr)  # Before mutating expr.
    global ____select_from_col = permutedims(reshape(collect(Iterators.flatten(val)), length(val),:))
    sym[] = ____select_from_col
    r = eval(expr)
    # Now turn the 3x1 Arrays back into a 3-el Vector
    if length(size(r)) == 2 && size(r)[end] == 1
        r = reshape(r, size(r)[1])
    end
    Table("RESULTS", [out_colname], [r])
end

""" select_from__group_by(t, groupby, colexprs...)

 Perform a SELECT query of `colexprs`, GROUP BY `groupby`.
 """
function select_from__group_by(t, groupby::Symbol, colexprs...)
    grouped_table = select_from(t, groupby)
    grouping_vals = grouped_table.cols
    @assert length(grouping_vals) == 1
    colors, counts = color_unique_vals(grouping_vals[1])
    num_colors = maximum(colors)

    results = []
    out_colnames = []
    for i in 1:length(colexprs)
        col = _retrieve_col_name(colexprs[i])
        inner_results, inner_colnames = _select_from__group_by_internal(t, col, colors, counts, num_colors)
        push!(results, inner_results...)
        push!(out_colnames, inner_colnames...)
    end
    Table("RESULTS", Tuple(out_colnames), Tuple(results))
end
select_from__group_by(t, groupby::Symbol, colexprs::Tuple) =
    select_from__group_by(t, groupby, colexprs...)
select_from__group_by(t, groupby::Symbol, colexprs::Array) =
    select_from__group_by(t, groupby, colexprs...)

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

# Types for dispatching to handle column version expression.
struct Column
    name::Symbol
end
struct ColumnExprRef
    name::Symbol
    sym
    expr::Expr
end
_retrieve_col_name(col::Symbol) = Column(col)
function _retrieve_col_name(expr::Expr)
    ColumnExprRef(expr_to_col(expr)..., expr)
end
_retrieve_col_names(colexprs...) =
    [_retrieve_col_name(col) for col in colexprs]

function _select_from__group_by_internal(t, colexpr::Column, colors, counts, num_colors)
    col = colexpr.name
    vals_table = select_from(t, col)
    vals = vals_table.cols

    results = []
    out_colnames = vals_table.headers
    for v in vals
        splits = [Vector{eltype(v)}(undef, c) for c in counts]
        for i in 1:num_colors
            splits[i] .= v[colors.==i]
        end

        row_results = []
        for val in splits
            push!(row_results, val[1])
        end
        push!(results, row_results)
    end
    return results, out_colnames
end

function _select_from__group_by_internal(t, colexpr::ColumnExprRef, colors, counts, num_colors)
    col, sym, expr = colexpr.name, colexpr.sym, colexpr.expr
    val_table = select_from(t, col)
    vals = val_table.cols

    results = []
    out_colnames = [tostring(expr)]
    for v in vals
        splits = [Vector{eltype(v)}(undef, c) for c in counts]
        for i in 1:num_colors
            splits[i] .= v[colors.==i]
        end

        row_results = []
        for val in splits
            out_t = _eval_expr_internal(val, sym, expr)
            push!(row_results, out_t.cols...)
        end
        push!(results, row_results)
    end
    return results, out_colnames
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
function macro_select(colexpr, fromexpr::Expr)
    colexpr = quote_column_syms(colexpr)
    @assert(fromexpr.args[1] == Symbol("@FROM"))
    if length(fromexpr.args) < 4
        return esc(quote
            select_from($(fromexpr.args[3]), $colexpr)
        end)
    end
    extraexpr = fromexpr.args[4]
    @assert isa(extraexpr, Expr)
    if extraexpr.args[1] == Symbol("@GROUP") && extraexpr.args[3].args[1] == Symbol("@BY")
        groupcolexpr = quote_column_syms(extraexpr.args[3].args[3])
        return esc(quote
            select_from__group_by($(fromexpr.args[3]), $groupcolexpr, $colexpr)
        end)
    end
end
macro SELECT(colexpr, fromexpr::Expr)
    return macro_select(colexpr, fromexpr)
end

"""
        @SQL begin
            @SELECT ...
            @FROM ...
        end
    Macro to create multiline SQL statements. Note that each internal macro must
    have at least one argument on the same line (Can't do `@SELECT
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
