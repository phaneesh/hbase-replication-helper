# To execute script launch this command on shell: hbase shell snapshot-tables.rb
@snapshot_list = Array.new
@copy_table_list = Array.new

include Java
java_import org.apache.hadoop.hbase.HBaseConfiguration
java_import org.apache.hadoop.hbase.client.HBaseAdmin

@conf = org.apache.hadoop.hbase.HBaseConfiguration.create

@admin = org.apache.hadoop.hbase.client.HBaseAdmin.new(@conf)

def cleanup(snapshot_name)
  puts "Deleting snapshot: #{snapshot_name}..."
  @admin.deleteSnapshot(snapshot_name)
  puts "Deleted snapshot: #{snapshot_name}."
end

snapshots = File.readlines("snapshots-list.txt")
puts '-----------------------------------------------------'
puts 'Snapshots to delete:'
puts snapshots
puts '-----------------------------------------------------'
puts 'Starting deletion.....'

snapshots.each do |s|
  cleanup(s)
end
puts '-----------------------------------------------------'
puts 'Done'
puts '-----------------------------------------------------'

exit 0

