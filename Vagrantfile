# -*- mode: ruby -*-
# vi: set ft=ruby :

# Vagrantfile API/syntax version. Don't touch unless you know what you're doing!
VAGRANTFILE_API_VERSION = "2"

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  config.vm.box = "hashicorp/precise64"

  config.vm.provision :shell, path: "vagrant/provision.sh"

  config.vm.network :forwarded_port, guest: 6667, host: 6667
  config.vm.network :forwarded_port, guest: 6668, host: 6668
  config.vm.network :forwarded_port, guest: 6669, host: 6669
  config.vm.network :forwarded_port, guest: 6670, host: 6670
  config.vm.network :forwarded_port, guest: 6671, host: 6671

  config.vm.network :forwarded_port, guest: 2181, host: 2181
	config.vm.network :forwarded_port, guest: 2182, host: 2182
	config.vm.network :forwarded_port, guest: 2183, host: 2183
	config.vm.network :forwarded_port, guest: 2184, host: 2184
	config.vm.network :forwarded_port, guest: 2185, host: 2185

  config.vm.network "private_network", ip: "192.168.100.67"

  config.vm.provider "vmware_fusion" do |v|
    v.vmx["memsize"] = "3072"
  end

  config.vm.provider :virtualbox do |vb, override|
    #vb.gui = true
    vb.memory = 3072
    vb.customize ['modifyvm', :id, '--cpus', '1']
    vb.customize ['modifyvm', :id, '--cpuexecutioncap', '100']
    vb.customize ['modifyvm', :id, '--nictype1', 'Am79C970A']
    vb.customize ['modifyvm', :id, '--chipset', 'ich9']
  end

end
