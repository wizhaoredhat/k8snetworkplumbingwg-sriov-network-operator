package v1_test

import (
	"fmt"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "github.com/k8snetworkplumbingwg/sriov-network-operator/api/v1"
	"github.com/k8snetworkplumbingwg/sriov-network-operator/pkg/consts"
)

func policyResourceValidator(provider v1.ResourceProvider) *v1.PolicyResourceValidator {
	config := &v1.SriovOperatorConfig{Status: v1.SriovOperatorConfigStatus{ResourceProvider: provider}}
	validator, err := config.NewPolicyResourceValidator()
	Expect(err).NotTo(HaveOccurred())
	return validator
}

var _ = Describe("PolicyResourceValidator", func() {
	DescribeTable("ignores the deprecated default policy while validating active policies", func(provider v1.ResourceProvider) {
		validator := policyResourceValidator(provider)
		deprecated := &v1.SriovNetworkNodePolicy{ObjectMeta: metav1.ObjectMeta{Name: consts.DefaultPolicyName}}
		Expect(validator.Add(deprecated)).To(Succeed())
		deprecated.Spec = v1.SriovNetworkNodePolicySpec{
			ResourceName: "SHARED", DeviceType: consts.DeviceTypeVfioPci,
			NicSelector: v1.SriovNetworkNicSelector{PfNames: []string{"ens1f0#2-5"}, NetFilter: "aws/NetworkID:net-1"},
		}
		Expect(validator.Add(deprecated)).To(Succeed())

		active := &v1.SriovNetworkNodePolicy{ObjectMeta: metav1.ObjectMeta{Name: "active"}}
		Expect(validator.Add(active)).To(MatchError(ContainSubstring("resource name must not be empty")))
		active.Spec.ResourceName = "shared"
		Expect(validator.Add(active)).To(Succeed())
		colliding := active.DeepCopy()
		colliding.Name = "colliding"
		colliding.Spec.ResourceName = "SHARED"
		if provider == v1.DRAResourceProvider() {
			Expect(validator.Add(colliding)).To(MatchError(ContainSubstring("normalizes to the same DRA device class name")))
			Expect(validator.Issue(active.Name)).To(MatchError(ContainSubstring("normalizes to the same DRA device class name")))
			Expect(validator.Issue(colliding.Name)).To(MatchError(ContainSubstring("normalizes to the same DRA device class name")))
		} else {
			Expect(validator.Add(colliding)).To(Succeed())
			Expect(validator.Issue(active.Name)).NotTo(HaveOccurred())
			Expect(validator.Issue(colliding.Name)).NotTo(HaveOccurred())
		}

		drivers, err := v1.PolicyResourceDrivers([]v1.SriovNetworkNodePolicy{*deprecated, *active})
		Expect(err).NotTo(HaveOccurred())
		Expect(drivers).To(Equal(map[string]string{"shared": ""}))
	},
		Entry("DRA", v1.DRAResourceProvider()),
		Entry("device plugin", v1.DevicePluginResourceProvider()),
	)

	DescribeTable("validates DRA-only selectors for the accepted provider", func(provider v1.ResourceProvider, policy v1.SriovNetworkNodePolicy, wantError string) {
		validator := policyResourceValidator(provider)
		err := validator.Add(&policy)
		if wantError != "" {
			Expect(err).To(MatchError(ContainSubstring(wantError)))
			Expect(validator.Issue(policy.Name)).To(MatchError(ContainSubstring(wantError)))
			return
		}
		Expect(err).NotTo(HaveOccurred())
		Expect(validator.Issue(policy.Name)).NotTo(HaveOccurred())
	},
		Entry("DRA accepts a whole PF", v1.DRAResourceProvider(), v1.SriovNetworkNodePolicy{
			ObjectMeta: metav1.ObjectMeta{Name: "whole-pf"},
			Spec:       v1.SriovNetworkNodePolicySpec{ResourceName: "nic", NicSelector: v1.SriovNetworkNicSelector{PfNames: []string{"ens1f0"}}},
		}, ""),
		Entry("DRA rejects a VF range", v1.DRAResourceProvider(), v1.SriovNetworkNodePolicy{
			ObjectMeta: metav1.ObjectMeta{Name: "ranged-pf"},
			Spec:       v1.SriovNetworkNodePolicySpec{ResourceName: "nic", NicSelector: v1.SriovNetworkNicSelector{PfNames: []string{"ens1f0#2-5"}}},
		}, "VF range"),
		Entry("DRA rejects netFilter", v1.DRAResourceProvider(), v1.SriovNetworkNodePolicy{
			ObjectMeta: metav1.ObjectMeta{Name: "net-filter"},
			Spec:       v1.SriovNetworkNodePolicySpec{ResourceName: "nic", NicSelector: v1.SriovNetworkNicSelector{NetFilter: "aws/NetworkID:net-1"}},
		}, "netFilter selector is not supported in DRA mode"),
		Entry("device plugin accepts DRA-only selectors", v1.DevicePluginResourceProvider(), v1.SriovNetworkNodePolicy{
			ObjectMeta: metav1.ObjectMeta{Name: "device-plugin"},
			Spec:       v1.SriovNetworkNodePolicySpec{ResourceName: "nic", NicSelector: v1.SriovNetworkNicSelector{PfNames: []string{"ens1f0#2-5"}, NetFilter: "aws/NetworkID:net-1"}},
		}, ""),
		Entry("DRA accepts a 63-character resource name", v1.DRAResourceProvider(), v1.SriovNetworkNodePolicy{
			ObjectMeta: metav1.ObjectMeta{Name: "max-length-resource-name"},
			Spec:       v1.SriovNetworkNodePolicySpec{ResourceName: strings.Repeat("a", v1.ResourceNameMaxLen)},
		}, ""),
		Entry("DRA rejects a resource name longer than 63 characters", v1.DRAResourceProvider(), v1.SriovNetworkNodePolicy{
			ObjectMeta: metav1.ObjectMeta{Name: "over-length-resource-name"},
			Spec:       v1.SriovNetworkNodePolicySpec{ResourceName: strings.Repeat("a", v1.ResourceNameMaxLen+1)},
		}, "no more than 63"),
		Entry("device plugin rejects a resource name longer than 63 characters", v1.DevicePluginResourceProvider(), v1.SriovNetworkNodePolicy{
			ObjectMeta: metav1.ObjectMeta{Name: "device-plugin-over-length-resource-name"},
			Spec:       v1.SriovNetworkNodePolicySpec{ResourceName: strings.Repeat("a", v1.ResourceNameMaxLen+1)},
		}, "no more than 63"),
	)

	It("preserves a valid 63-character resource name and rejects longer names", func() {
		validName := strings.Repeat("a", v1.ResourceNameMaxLen)
		drivers, err := v1.PolicyResourceDrivers([]v1.SriovNetworkNodePolicy{{
			ObjectMeta: metav1.ObjectMeta{Name: "valid-max-length"},
			Spec:       v1.SriovNetworkNodePolicySpec{ResourceName: validName},
		}})
		Expect(err).NotTo(HaveOccurred())
		Expect(drivers).To(Equal(map[string]string{validName: ""}))

		_, err = v1.PolicyResourceDrivers([]v1.SriovNetworkNodePolicy{{
			ObjectMeta: metav1.ObjectMeta{Name: "invalid-over-length"},
			Spec:       v1.SriovNetworkNodePolicySpec{ResourceName: strings.Repeat("a", v1.ResourceNameMaxLen+1)},
		}})
		Expect(err).To(MatchError(ContainSubstring("no more than 63")))
	})

	DescribeTable("collects compatible allocation drivers for a shared resource", func(deviceTypes []string, wantDriver string, wantError bool) {
		policies := make([]v1.SriovNetworkNodePolicy, 0, len(deviceTypes))
		for i, deviceType := range deviceTypes {
			name := fmt.Sprintf("worker-%d", i)
			policies = append(policies, v1.SriovNetworkNodePolicy{
				ObjectMeta: metav1.ObjectMeta{Name: name},
				Spec: v1.SriovNetworkNodePolicySpec{ResourceName: "shared", DeviceType: deviceType,
					NodeSelector: map[string]string{"kubernetes.io/hostname": name}},
			})
		}

		drivers, err := v1.PolicyResourceDrivers(policies)
		if wantError {
			Expect(err).To(MatchError(ContainSubstring("incompatible deviceType")))
			return
		}
		Expect(err).NotTo(HaveOccurred())
		Expect(drivers).To(Equal(map[string]string{"shared": wantDriver}))
	},
		Entry("Intel and Mellanox netdevices", []string{"netdevice", ""}, "", false),
		Entry("VFIO", []string{"vfio-pci", "vfio-pci"}, "vfio-pci", false),
		Entry("mixed drivers", []string{"netdevice", "vfio-pci"}, "", true),
	)

	It("attributes a shared conflict to every policy in the class without spreading local errors", func() {
		validator := policyResourceValidator(v1.DRAResourceProvider())
		first := &v1.SriovNetworkNodePolicy{
			ObjectMeta: metav1.ObjectMeta{Name: "first"},
			Spec:       v1.SriovNetworkNodePolicySpec{ResourceName: "shared", DeviceType: consts.DeviceTypeNetDevice},
		}
		conflicting := first.DeepCopy()
		conflicting.Name = "conflicting"
		conflicting.Spec.DeviceType = consts.DeviceTypeVfioPci
		later := first.DeepCopy()
		later.Name = "later"

		Expect(validator.Add(first)).To(Succeed())
		Expect(validator.Add(conflicting)).To(MatchError(ContainSubstring("incompatible deviceType")))
		Expect(validator.Add(later)).To(MatchError(ContainSubstring("incompatible deviceType")))
		for _, name := range []string{first.Name, conflicting.Name, later.Name} {
			Expect(validator.Issue(name)).To(MatchError(ContainSubstring("incompatible deviceType")))
		}

		local := &v1.SriovNetworkNodePolicy{
			ObjectMeta: metav1.ObjectMeta{Name: "local"},
			Spec: v1.SriovNetworkNodePolicySpec{ResourceName: "separate",
				NicSelector: v1.SriovNetworkNicSelector{PfNames: []string{"ens1f0#2-5"}}},
		}
		healthy := local.DeepCopy()
		healthy.Name = "healthy"
		healthy.Spec.NicSelector.PfNames = []string{"ens1f0"}
		Expect(validator.Add(local)).To(MatchError(ContainSubstring("VF range")))
		Expect(validator.Add(healthy)).To(Succeed())
		Expect(validator.Issue(local.Name)).To(MatchError(ContainSubstring("VF range")))
		Expect(validator.Issue(healthy.Name)).NotTo(HaveOccurred())
	})
})

var _ = Describe("ResourceNameToDeviceClassName", func() {
	DescribeTable("normalizes resource names", func(resourceName, expected string) {
		Expect(v1.ResourceNameToDeviceClassName(resourceName)).To(Equal(expected))
	},
		Entry("empty returns sriov", "", "sriov"),
		Entry("underscores to dashes", "intel_nic", "intel-nic"),
		Entry("uppercase to lowercase", "IntelNic", "intelnic"),
		Entry("mixed", "My_Resource_01", "my-resource-01"),
		Entry("trailing hyphen trimmed", "a1-", "a1"),
		Entry("non-DNS chars stripped", "foo.bar", "foobar"),
		Entry("leading/trailing hyphens trimmed", "-x-", "x"),
		Entry("leading underscores become hyphens then trimmed", "_foo", "foo"),
		Entry("only underscores/dashes becomes sriov", "___", "sriov"),
	)
})
