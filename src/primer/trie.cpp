#include "primer/trie.h"
#include <strings.h>
#include <cstddef>
#include <memory>
#include <string_view>
#include <utility>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  if (root_ == nullptr) {
    return nullptr;
  }

  auto cur = root_;
  for (char c : key) {
    if (cur->children_.find(c) != cur->children_.end()) {
      // find the path
      cur = cur->children_.at(c);
    } else {
      return nullptr;
    }
  }
  auto ret = dynamic_cast<const TrieNodeWithValue<T> *>(cur.get());
  if (ret == nullptr) {
    return nullptr;
  }
  return ret->value_.get();
}

// dfs for put function
template <class T>
std::shared_ptr<const TrieNode> PutDfs(const std::shared_ptr<const TrieNode> &root, std::string_view key, size_t index,
                                       std::shared_ptr<T> value_p) {
  if (index == key.size()) {
    return std::make_shared<TrieNodeWithValue<T>>(TrieNodeWithValue<T>(root->children_, value_p));
  }

  std::shared_ptr<const TrieNode> new_node;
  auto new_root = root->Clone();

  if (root->children_.find(key[index]) != root->children_.end()) {
    new_node = PutDfs(new_root->children_.at(key[index]), key, index + 1, value_p);
  } else {
    new_node = PutDfs(std::make_shared<TrieNode>(TrieNode()), key, index + 1, value_p);
  }

  new_root->children_[key[index]] = new_node;
  return std::shared_ptr<TrieNode>(std::move(new_root));
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
  std::shared_ptr<T> value_p = std::make_shared<T>(std::move(value));
  std::shared_ptr<const TrieNode> root;

  if (root_ == nullptr) {
    std::shared_ptr<const TrieNode> tn = std::make_shared<TrieNode>(TrieNode());
    root = PutDfs(tn, key, 0, value_p);
  } else {
    root = PutDfs(root_, key, 0, value_p);
  }
  return Trie(root);
}

// dfs for remove function
std::shared_ptr<const TrieNode> RemoveDfs(const std::shared_ptr<const TrieNode> &root, std::string_view key,
                                          size_t index) {
  if (index == key.size()) {
    if (root->children_.empty()) {
      return nullptr;
    }
    return std::make_shared<TrieNode>(TrieNode(root->children_));
  }

  std::shared_ptr<const TrieNode> new_node;
  if (root->children_.find(key[index]) != root->children_.end()) {
    new_node = RemoveDfs(root->children_.at(key[index]), key, index + 1);
    auto new_root = root->Clone();
    if (new_node) {
      new_root->children_[key[index]] = new_node;
    } else {
      new_root->children_.erase(key[index]);
      // delete unnecessary node
      if (!new_root->is_value_node_ && new_root->children_.empty()) {
        return nullptr;
      }
    }
    return new_root;
  }
  return root;
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
  auto root = RemoveDfs(root_, key, 0);
  return Trie(root);
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
