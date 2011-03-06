module Parse
  def parse(data)
   data.collect {|row| row.to_s.split(',')}
  end
end

class FileSplitter
  include Java
  require 'thread'
  include Parse

  def initialize(file_name)
    @file_name = file_name
  end

  def number_of_cores
    java.lang.Runtime.getRuntime.availableProcessors
  end

  def map
    @threads = []
    start = 0
    size = file_handle.size/2
    (1..number_of_cores).each do |core|
      @threads << Thread.new(file_handle[start..size]) {|data|
        parse(data)
        start = size
        size = size * 2
      }
    end
  end

  def reduce
    @threads.each { |aThread|  aThread.join }
  end

  def split
    map
    reduce
  end

private
  def file_handle
    File.readlines(@file_name)
  end
end

fs = FileSplitter.new("Desktop/csv_data.txt")
fs.split
