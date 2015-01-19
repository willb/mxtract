require "rubygems"

require "fileutils"
require "uri"

require "thor"
require "mongo"
require "json"


module CLIHelpers
  def symbolize_hash(hash)
    hash.inject({}){|acc,(k,v)| acc[k.to_sym] = v; acc}
  end
  
  def check_output_dir(outputDir)
    if File.exists?(outputDir)
      if @options[:clobber]
        FileUtils.rm_rf(outputDir)
      else
        raise "#{outputDir} already exists; move it first"
      end
    end
  end
  
  def ensure_exists(path)
    unless File.exists?(path)
      FileUtils.mkdir_p(path)
    end
    path
  end
end

class MXtract < Thor
  desc "extract URL DB COLLECTION [...]", "extract data from listed DBs at URL"
  option :verbose, :type => :boolean, :desc => "provide additional debugging output"
  option :user, :type => :string, :desc => "remote username"
  option :password, :type => :string, :desc => "remote password"
  option :output_dir, :type => :string, :desc => "output dir"
  
  MAX_PER_FILE = 1000
  
  def extract(url, db, *colls)
    opts = symbolize_hash(options)
    uri = "mongodb://#{URI.escape(opts[:user], ":@")}:#{URI.escape(opts[:password], ":@")}@#{url}"
    mc = Mongo::MongoClient.from_uri(uri)
    the_db = mc.db(db)
    output_dir = opts[:output_dir] || "."
    
    ensure_exists(output_dir)
    
    colls.each do |cname|
      cl = the_db.collection(cname)
      count = 0
      idx = 0
      outf = nil
      
      cl.find.each do |obj|
        if count % MAX_PER_FILE == 0
          outf.close if outf
          idx, fn = next_fn(idx, "#{output_dir}/#{cname}")
          outf = open(fn, "w")
        end
        
        outf.write(obj.to_json)
        outf.write("\n")
        
        count = count + 1
      end
      
      outf.close if outf
    end
    
  end
  
  private
  
  def next_fn(idx, base)
    [idx+1, sprintf("#{base}-%07d.json", idx)] 
  end
  
  include CLIHelpers
end
