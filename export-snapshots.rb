# To execute script launch this command on shell: hbase shell snapshot-tables.rb
# Run this on source cluster

@destination_namenode = "destination name node host"
@hdp_stack_version = "2.6.5.0-292"

@clusterToSave = "hdfs:///#{@destination_namenode}:8082/hbase"
# CHECK THE PATH OF HBase lib
@libjars = `ls /usr/hdp/#{@hdp_stack_version}/hbase/lib/*.jar | tr "\n" ","`
@mappers = '16'
@bandwidth = '7500'

include Java
java_import org.apache.hadoop.hbase.HBaseConfiguration
java_import org.apache.hadoop.hbase.client.HBaseAdmin
java_import org.apache.hadoop.hbase.snapshot.ExportSnapshot
java_import org.apache.hadoop.util.ToolRunner

@conf = org.apache.hadoop.hbase.HBaseConfiguration.create

@admin = org.apache.hadoop.hbase.client.HBaseAdmin.new(@conf)

def export(snapshot_name)
  puts "\tExporting Snapshot: #{snapshot_name}"
  @es = org.apache.hadoop.hbase.snapshot.ExportSnapshot.new
  args = ["--libjars", @libjars, "-snapshot", snapshot_name, "-copy-to", @clusterToSave, "-mappers", @mappers, "-bandwidth", @bandwidth]
  java_args = args.to_java :String
  ToolRunner.run(@conf, @es, java_args)
  puts "\tDone exporting Snapshot: #{snapshot_name}"
end

snapshots = File.readlines("snapshots-list.txt")
puts '-----------------------------------------------------'
puts 'Snapshots to export:'
puts snapshots
puts '-----------------------------------------------------'
puts 'Starting export.....'

snapshots.each do |s|
  export(s)
end
puts '-----------------------------------------------------'
puts 'Done'
puts '-----------------------------------------------------'

exit 0

