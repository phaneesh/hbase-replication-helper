# To execute script launch this command on shell: hbase shell restore-snapshots.rb
# Run this on target cluster

include Java
java_import org.apache.hadoop.hbase.HBaseConfiguration
java_import org.apache.hadoop.hbase.client.HBaseAdmin
java_import org.apache.hadoop.hbase.snapshot.ExportSnapshot
java_import org.apache.hadoop.util.ToolRunner

@conf = org.apache.hadoop.hbase.HBaseConfiguration.create

@admin = org.apache.hadoop.hbase.client.HBaseAdmin.new(@conf)

snapshots = @admin.listSnapshots(".*-SNAPSHOT-.*").to_a
puts '-----------------------------------------------------'
puts 'Snapshots to restore:'
puts snapshots
puts '-----------------------------------------------------'
puts 'Starting restore.....'
snapshots.each { |s|
  puts "\tRestoring Snapshot: #{s.name}"
  @admin.restoreSnapshot(s.name)
}
puts '-----------------------------------------------------'
puts 'Done'
puts '-----------------------------------------------------'
exit 0
