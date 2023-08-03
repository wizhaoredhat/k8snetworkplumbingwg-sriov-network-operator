package generic

import (
	"bytes"
	"os/exec"
	"reflect"
	"strconv"
	"strings"
	"syscall"

	"github.com/golang/glog"

	sriovnetworkv1 "github.com/k8snetworkplumbingwg/sriov-network-operator/api/v1"
	constants "github.com/k8snetworkplumbingwg/sriov-network-operator/pkg/consts"
	"github.com/k8snetworkplumbingwg/sriov-network-operator/pkg/host"
	plugin "github.com/k8snetworkplumbingwg/sriov-network-operator/pkg/plugins"
	"github.com/k8snetworkplumbingwg/sriov-network-operator/pkg/utils"
)

var PluginName = "generic_plugin"

type GenericPlugin struct {
	PluginName           string
	SpecVersion          string
	DesireState          *sriovnetworkv1.SriovNetworkNodeState
	LastState            *sriovnetworkv1.SriovNetworkNodeState
	LoadVfioDriver       uint
	LoadVirtioVdpaDriver uint
	DesiredKernelParams  map[string]uint
	RunningOnHost        bool
	HostManager          host.HostManagerInterface
}

const scriptsPath = "bindata/scripts/enable-kargs.sh"

const (
	unloaded = iota
	loading
	loaded
)

// Initialize our plugin and set up initial values
func NewGenericPlugin(runningOnHost bool) (plugin.VendorPlugin, error) {
	return &GenericPlugin{
		PluginName:           PluginName,
		SpecVersion:          "1.0",
		LoadVfioDriver:       unloaded,
		LoadVirtioVdpaDriver: unloaded,
		DesiredKernelParams:  make(map[string]uint),
		RunningOnHost:        runningOnHost,
		HostManager:          host.NewHostManager(runningOnHost),
	}, nil
}

// Name returns the name of the plugin
func (p *GenericPlugin) Name() string {
	return p.PluginName
}

// Spec returns the version of the spec expected by the plugin
func (p *GenericPlugin) Spec() string {
	return p.SpecVersion
}

// OnNodeStateChange Invoked when SriovNetworkNodeState CR is created or updated, return if need dain and/or reboot node
func (p *GenericPlugin) OnNodeStateChange(new *sriovnetworkv1.SriovNetworkNodeState) (needDrain bool, needReboot bool, err error) {
	glog.Info("generic-plugin OnNodeStateChange()")
	needDrain = false
	needReboot = false
	err = nil
	p.DesireState = new

	needDrain = needDrainNode(new.Spec.Interfaces, new.Status.Interfaces)
	needReboot = p.needRebootNode(new)

	if needReboot {
		needDrain = true
	}
	return
}

// Apply config change
func (p *GenericPlugin) Apply() error {
	glog.Infof("generic-plugin Apply(): desiredState=%v", p.DesireState.Spec)
	if p.LoadVfioDriver == loading {
		if err := p.HostManager.LoadKernelModule("vfio_pci"); err != nil {
			glog.Errorf("generic-plugin Apply(): fail to load vfio_pci kmod: %v", err)
			return err
		}
		p.LoadVfioDriver = loaded
	}

	if p.LoadVirtioVdpaDriver == loading {
		if err := p.HostManager.LoadKernelModule("virtio_vdpa"); err != nil {
			glog.Errorf("generic-plugin Apply(): fail to load virtio_vdpa kmod: %v", err)
			return err
		}
		p.LoadVirtioVdpaDriver = loaded
	}

	if p.LastState != nil {
		glog.Infof("generic-plugin Apply(): lastStat=%v", p.LastState.Spec)
		if reflect.DeepEqual(p.LastState.Spec.Interfaces, p.DesireState.Spec.Interfaces) {
			glog.Info("generic-plugin Apply(): nothing to apply")
			return nil
		}
	}

	// Create a map with all the PFs we will need to configure
	// we need to create it here before we access the host file system using the chroot function
	// because the skipConfigVf needs the mstconfig package that exist only inside the sriov-config-daemon file system
	pfsToSkip, err := utils.GetPfsToSkip(p.DesireState)
	if err != nil {
		return err
	}

	// When calling from systemd do not try to chroot
	if !p.RunningOnHost {
		exit, err := utils.Chroot("/host")
		if err != nil {
			return err
		}
		defer exit()
	}

	if err := utils.SyncNodeState(p.DesireState, pfsToSkip); err != nil {
		return err
	}
	p.LastState = &sriovnetworkv1.SriovNetworkNodeState{}
	*p.LastState = *p.DesireState
	return nil
}

func needVfioDriver(state *sriovnetworkv1.SriovNetworkNodeState) bool {
	for _, iface := range state.Spec.Interfaces {
		for i := range iface.VfGroups {
			if iface.VfGroups[i].DeviceType == constants.DeviceTypeVfioPci {
				return true
			}
		}
	}
	return false
}

func needVirtioVdpaDriver(state *sriovnetworkv1.SriovNetworkNodeState) bool {
	for _, iface := range state.Spec.Interfaces {
		for i := range iface.VfGroups {
			if iface.VfGroups[i].VdpaType == constants.VdpaTypeVirtio {
				return true
			}
		}
	}
	return false
}

// trySetKernelParam Tries to add the kernel param via ostree or grubby.
func trySetKernelParam(kparam string) (bool, error) {
	glog.Info("generic-plugin trySetKernelParam()")
	var stdout, stderr bytes.Buffer
	cmd := exec.Command("/bin/sh", scriptsPath, kparam)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		// if grubby is not there log and assume kernel args are set correctly.
		if isCommandNotFound(err) {
			glog.Errorf("generic-plugin trySetKernelParam(): grubby or ostree command not found. Please ensure that kernel param %s are set", kparam)
			return false, nil
		}
		glog.Errorf("generic-plugin trySetKernelParam(): fail to enable kernel param %s: %v", kparam, err)
		return false, err
	}

	i, err := strconv.Atoi(strings.TrimSpace(stdout.String()))
	if err == nil {
		if i > 0 {
			glog.Infof("generic-plugin trySetKernelParam(): need to reboot node for kernel param %s", kparam)
			return true, nil
		}
	}
	return false, err
}

func isCommandNotFound(err error) bool {
	if exitErr, ok := err.(*exec.ExitError); ok {
		if status, ok := exitErr.Sys().(syscall.WaitStatus); ok && status.ExitStatus() == 127 {
			return true
		}
	}
	return false
}

// AddToDesiredKernelParams Should be called to queue a kernel param to be added to the node.
func (p *GenericPlugin) AddToDesiredKernelParams(kparam string) {
	if _, ok := p.DesiredKernelParams[kparam]; !ok {
		glog.Infof("generic-plugin AddToDesiredKernelParams(): Adding %s to desired kernel params", kparam)
		// element "uint" is a counter of number of attempts to set the kernel param
		p.DesiredKernelParams[kparam] = 0
	}
}

// SetAllDesiredKernelParams Should be called to set all the kernel parameters. Returns true if reboot of the node is needed.
func (p *GenericPlugin) SetAllDesiredKernelParams() bool {
	needReboot := false
	for kparam, attempts := range p.DesiredKernelParams {
		if !utils.IsKernelCmdLineParamSet(kparam, false) {
			if attempts > 0 && attempts <= 4 {
				glog.Errorf("generic-plugin SetAllDesiredKernelParams(): Fail to set kernel param %s after reboot with attempts %d", kparam, attempts)
			} else if attempts > 4 {
				// If we tried several times and was unsuccessful, we should give up.
				continue
			}
			update, err := trySetKernelParam(kparam)
			if err != nil {
				glog.Errorf("generic-plugin SetAllDesiredKernelParams(): Fail to set kernel param %s: %v", kparam, err)
			}
			if update {
				glog.V(2).Infof("generic-plugin SetAllDesiredKernelParams(): Need reboot for setting kernel param %s", kparam)
			}
			// Update the number of attempts we tried to set the kernel parameter.
			p.DesiredKernelParams[kparam]++
			needReboot = needReboot || update
		}
	}
	return needReboot
}

func needDrainNode(desired sriovnetworkv1.Interfaces, current sriovnetworkv1.InterfaceExts) (needDrain bool) {
	glog.V(2).Infof("generic-plugin needDrainNode(): current state '%+v', desired state '%+v'", current, desired)
	needDrain = false
	for _, ifaceStatus := range current {
		configured := false
		for _, iface := range desired {
			if iface.PciAddress == ifaceStatus.PciAddress {
				configured = true
				if ifaceStatus.NumVfs == 0 {
					glog.V(2).Infof("generic-plugin needDrainNode(): no need drain, for PCI address %s current NumVfs is 0", iface.PciAddress)
					break
				}
				if utils.NeedUpdate(&iface, &ifaceStatus) {
					glog.V(2).Infof("generic-plugin needDrainNode(): need drain, for PCI address %s request update", iface.PciAddress)
					needDrain = true
					return
				}
				glog.V(2).Infof("generic-plugin needDrainNode(): no need drain,for PCI address %s expect NumVfs %v, current NumVfs %v", iface.PciAddress, iface.NumVfs, ifaceStatus.NumVfs)
			}
		}
		if !configured && ifaceStatus.NumVfs > 0 {
			glog.V(2).Infof("generic-plugin needDrainNode(): need drain, %v needs to be reset", ifaceStatus)
			needDrain = true
			return
		}
	}
	return
}

func (p *GenericPlugin) needRebootNode(state *sriovnetworkv1.SriovNetworkNodeState) (needReboot bool) {
	needReboot = false
	if p.LoadVfioDriver != loaded {
		if needVfioDriver(state) {
			p.LoadVfioDriver = loading
			p.AddToDesiredKernelParams(utils.KernelParamIntelIommu)
			p.AddToDesiredKernelParams(utils.KernelParamIommuPt)
		}
	}

	if p.LoadVirtioVdpaDriver != loaded {
		if needVirtioVdpaDriver(state) {
			p.LoadVirtioVdpaDriver = loading
		}
	}

	update, err := utils.WriteSwitchdevConfFile(state)
	if err != nil {
		glog.Errorf("generic-plugin needRebootNode(): fail to write switchdev device config file")
	}
	if update {
		glog.V(2).Infof("generic-plugin needRebootNode(): need reboot for updating switchdev device configuration")
	}
	needReboot = needReboot || update || p.SetAllDesiredKernelParams()
	return
}
