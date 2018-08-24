include("../src/SimpleSQL.jl")
using .SimpleSQL
using Test

# CREATE TABLE
create_table("hi", ("id",Int))
t = create_table("people", ("id",Int), ("name", String))

# INSERT INTO
insert_into_values(t, (1,"sarah"))
insert_into_values(t, (2,"nathan"))
insert_into_values(t, (3,"max"))

# SELECT
# column name
@test select_from(t, "name").cols == permutedims(["sarah" "nathan" "max"])
@test select_from(t, "*").cols == [1 "sarah"; 2 "nathan"; 3 "max"]
@test select_from(t, :*) == select_from(t, "*")
select_from(t, [:name, :id])
select_from(t, :*, "name")

# internals
col,sym = SimpleSQL.expr_to_col(t, :(sum(id))); @test col == :id && sym isa Base.RefArray
col,sym = SimpleSQL.expr_to_col(t, :(unique(name))); @test col == :name && sym isa Base.RefArray
col,sym = SimpleSQL.expr_to_col(t, :(length(*))); @test col == :* && sym isa Base.RefArray

# expressions
@test select_from(t, :(sum("id"))).cols == select_from(t, :(sum(id))).cols
@test all(select_from(t, :(sum(id))).cols .== [6])
@test select_from(t, [:(sum(id)), :(length(name))]).cols == [6 3]
select_from(t, :(sum(id)), :(uppercase.(name)))
select_from(t, :(uppercase.(name)))
select_from(t, :id, :(sum(id)))
select_from(t, :(length(id)))
# TODO: this one is wrong. The system currently assumes every column name returns a column, but "*" returns all columns.
# This should be 3 not 6.
select_from(t, :(length(*)))
@test_broken select_from(t, :(length(*))) == select_from(t, :(length(id)))

t2 = create_table("nums", ("id",Int), ("n", Int))
insert_into_values(t2, (2,3))
insert_into_values(t2, (3,5))

# Oh, this isn't supposed to work! So what, SUM() always has to take a column?
select_from(t2, :(sum(*)))
@test_broken select_from(t2, :(:aisle .+ 2))  # Not sure why this one doesn't work..

# GROUP BY
# internal
@test SimpleSQL._retrieve_col_name(t, :id) isa SimpleSQL.Column
@test SimpleSQL._retrieve_col_name(t, :(sum(id))) isa SimpleSQL.ColumnExprRef

groceries = create_table("groceries", (:id, Int), (:name, String), (:quantity, Int), (:aisle, Int))
insert_into_values(groceries, (1, "Bananas", 34, 7))
insert_into_values(groceries, (2, "Peanut Butter", 1, 2))
insert_into_values(groceries, (3, "Dark Chocolate Bars", 2, 2))
insert_into_values(groceries, (4, "Ice cream", 1, 12))
insert_into_values(groceries, (5, "Cherries", 6, 2))
insert_into_values(groceries, (6, "Chocolate syrup", 1, 4))


@test select_from(groceries, :(sum(quantity)), where=:(aisle .== 2)).cols[1] == 9
@test select_from(groceries, :aisle, where=:(aisle .> 2)).cols == permutedims([7 12 4])

select_from(groceries, :name; groupby=:aisle)
select_from(groceries, :aisle, :(sum(quantity)); groupby=:aisle)
# This one is wrong...
@test_broken select_from(groceries, :(identity(aisle)); groupby=:aisle) == select_from(groceries, :aisle; groupby=:aisle)


# MACRO SQL SYNTAX
@CREATE @TABLE favorite_books (:id, Int), ("name", String), (:rating, Real)
@INSERT @INTO favorite_books @VALUES (1, "Eragon", 3.5)

@SELECT :* @FROM favorite_books
@SELECT :id, :aisle, :quantity @FROM groceries
@SELECT id, aisle @FROM groceries
@SELECT (:aisle, :(sum(quantity))) @FROM groceries
@SELECT :aisle, :(sum(quantity)) @FROM groceries @GROUP @BY :aisle
@SELECT aisle, sum(quantity) @FROM groceries @GROUP @BY aisle

@SELECT :id, :aisle @FROM groceries
@SELECT id, aisle @FROM groceries
@SELECT aisle, :(sum(quantity)) @FROM groceries
@SELECT aisle, sum(quantity) @FROM groceries

@SELECT name, sum(quantity) @FROM groceries @GROUP @BY aisle
@SELECT sum(quantity) @FROM groceries @GROUP @BY aisle
x = 2
@SELECT aisle, aisle .+ $x*2 @FROM groceries

@SELECT aisle, length(aisle), sum(quantity) @FROM groceries @GROUP @BY aisle

# WHERE
prices = [2.50, 4.25]
x = @SQL begin
    @SELECT name, quantity, $prices .* quantity
    @FROM groceries
    @WHERE occursin.(Ref(r"Chocolate"), name)
end
@test x.cols == ["Dark Chocolate Bars" 2  5.0
                 "Chocolate syrup"     1  4.25]

@SQL begin
    @SELECT sum(quantity),
                aisle
    @FROM groceries
    @GROUP
        @BY aisle
end

# Evaling custom functions:
foo(x) = x.+2
@SELECT foo(aisle) @FROM groceries

using Statistics
@SELECT aisle, median(quantity) @FROM groceries @GROUP @BY aisle


# timing...
#t = SQL.Table("t", (:a, :b), (rand(1:10, 1000000), rand(1:10, 1000000)));
#@time t = SQL.Table("t", (:a, :b), (rand(1:10, 1000000), rand(1:10, 1000000)));
#@time @SELECT b, :(sum(a)) @FROM t @GROUP @BY b;
#@time @SELECT b, sum(a) @FROM t @GROUP @BY b;

# Disk I/O
f = joinpath(tempdir(), "groceries")
write_table_to_disk(groceries, f)
groceries2 = read_table_from_disk(f)

x = @SQL begin
    @SELECT sum(quantity), aisle
    @FROM groceries2 @GROUP @BY aisle
end

@test x.headers == ["sum(quantity)", :aisle]
@test x.cols == [34 7; 9 2; 1 12; 1 4]
