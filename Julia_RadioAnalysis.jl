using HTTP
using DataFrames, JSONTables

source = HTTP.get("https://nowplaying.bbgi.com/WMMRFM/list?limit=200&amp;offset=0")

information = String(source.body)
jt = jsontable(information)
df = DataFrame(jt)

select!(df,Not(:id))
select!(df,Not(:site))
select!(df,Not(:station))

insertcols!(df, 3, :date => df.createdOn)
insertcols!(df, 4, :time => df.createdOn)

df.date = SubString.(df.date, 1,10)
df.time = SubString.(df.time, 12,23)

#=
let
    numRepeats = 0
    for iter in 1:length(df.title)
        repeats = count(i->i==df.title[iter], df.title)
        if (repeats > 1)
            numRepeats += 1
            println(df.title[iter], " by ", df.artist[iter], " repeated ", repeats, " times")
        end
        end
    println("----- ", numRepeats, " songs repeated -----")
    end
=#
#= Can't just go by unique time, also need to go by unique date
for iter in 1:length(unique(df.time))
    println(filter(i->i==unique(df.time)[iter],df.time))
end
=#