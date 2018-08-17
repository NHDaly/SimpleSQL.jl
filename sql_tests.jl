include("sql.jl")
using .SQL

# CREATE TABLE
create_table("hi", ("id",Int))
t = create_table("people", ("id",Int), ("name", String))

# INSERT INTO
insert_into_values(t, (1,"sarah"))
insert_into_values(t, (2,"nathan"))
insert_into_values(t, (3,"max"))

# SELECT
# column name
select_from(t, "name")
select_from(t, "*")
select_from(t, :*)
select_from(t, [:name, :id])
select_from(t, :*, "name")

# internals
SQL.expr_to_cols([:(sum(id)), :(unique(name))])
SQL.expr_to_col(:(length(*)))

# expressions
select_from(t, :(sum("id")))
select_from(t, [:(sum(id)), :(length(name))])
select_from(t, :(sum(id)), :(uppercase.(name)))
select_from(t, :(uppercase.(name)))
select_from(t, :id, :(sum(id)))
select_from(t, :(length(id)))
# TODO: this one is wrong. The system currently assumes every column name returns a column, but "*" returns all columns.
select_from(t, :(length(*)))

t2 = create_table("nums", ("id",Int), ("n", Int))
insert_into_values(t2, (2,3))
insert_into_values(t2, (3,5))

# Oh, this isn't supposed to work! So what, SUM() always has to take a column?
select_from(t2, :(sum(*)))
#select_from(t2, :(:aisle .+ 2))  # Not sure why this one doesn't work..

# GROUP BY
# internal
SQL._retrieve_col_names(:id)
SQL._retrieve_col_names(:id, :name)
SQL._retrieve_col_names(:(sum(id)))
SQL._retrieve_col_names(:(sum(id)), :name)

groceries = create_table("groceries", (:id, Int), (:name, String), (:quantity, Int), (:aisle, Int))
insert_into_values(groceries, (1, "Bananas", 56, 7))
insert_into_values(groceries, (2, "Peanut Butter", 1, 2))
insert_into_values(groceries, (3, "Dark Chocolate Bars", 2, 2))
insert_into_values(groceries, (4, "Ice cream", 1, 12))
insert_into_values(groceries, (5, "Cherries", 6, 2))
insert_into_values(groceries, (6, "Chocolate syrup", 1, 4))


select_from__group_by(groceries, :aisle, :name)
select_from__group_by(groceries, :aisle, :aisle, :(sum(quantity)))
select_from__group_by(groceries, :aisle, :(identity(aisle)))  # 😮

# MACRO SQL SYNTAX
@CREATE @TABLE favorite_books ((:id, Int), ("name", String), (:rating, Real))
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

@SELECT aisle, aisle .+ $x*2 @FROM groceries

@SQL begin
    @SELECT sum(quantity),
                aisle
    @FROM groceries
    @GROUP
        @BY aisle
end

# timing...
t = SQL.Table("t", (:a, :b), (rand(1:10, 1000000), rand(1:10, 1000000)));
@time t = SQL.Table("t", (:a, :b), (rand(1:10, 1000000), rand(1:10, 1000000)));
@time @SELECT b, :(sum(a)) @FROM t @GROUP @BY b;
@time @SELECT b, sum(a) @FROM t @GROUP @BY b;

# For if I use arrays in the RESULT table output instead
#function Base.show(io::IO, t::Table)
#    str = t.title *"\n"*
#        " "*join(t.headers, "\t| ") *"\n"*
#        join(["-"^(length(tostring(h))+1) for h in t.headers], "\t|") *"\n";
#    print(io, str)
#    Base.print_array(stdout, t.cols)
#end
